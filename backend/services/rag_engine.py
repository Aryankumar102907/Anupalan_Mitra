"""
rag_engine.py  -  Optimised Agentic RAG pipeline
Gemini 2.5 Flash + ChromaDB (ISO knowledge base) + pdfplumber (company docs)

Speed design (Big4 best practices):
  * ONE batch Gemini call evaluates ALL clauses at once  ->  N x faster
  * Concurrent asyncio ChromaDB lookups via asyncio.gather
  * Company context built once, reused across all clauses
  * Structured JSON array response - no per-clause round-trips
"""

import os
import re
import json
import asyncio
import time
import pdfplumber
import chromadb
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

# Resolve to backend/chroma_db regardless of whether this file lives in
# services/ or directly under backend/.  __file__ = .../backend/services/rag_engine.py
CHROMA_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "chroma_db"))
COLLECTION_NAME = "iso_frameworks"
API_KEY         = os.getenv("GOOGLE_API_KEY")

if not API_KEY:
    raise EnvironmentError("GOOGLE_API_KEY is missing. Add it to backend/.env")

genai.configure(api_key=API_KEY)

# User requested gemini-2.5-flash (Note: 20 requests/day free tier limit)
GEMINI_MODEL  = "gemini-2.5-flash"
_GEN_CONFIG   = {
    "temperature": 0.1, 
    "max_output_tokens": 8192,
    "response_mime_type": "application/json"
}
llm           = genai.GenerativeModel(model_name=GEMINI_MODEL, generation_config=_GEN_CONFIG)
CHUNK_SIZE    = 5   # clauses per Gemini call — keeps output well within token limits
print(f"[Gemini] Model: {GEMINI_MODEL}  (20 req/day free tier)")


# Shared ONNX embedding function - same model as knowledge_base_loader.py
_ef = DefaultEmbeddingFunction()

# =============================================================================
#  ISO CHECKLISTS
# =============================================================================

ISO_CHECKLISTS: dict[str, list[dict]] = {

    # -- ISO 37001:2016  Anti-bribery Management Systems ----------------------
    "iso37001": [
        {"clause": "4.1",  "title": "Understanding the organization and its context",
         "requirement": "The organization has identified and documented the internal and external issues relevant to its purpose that affect the ABMS, including bribery risk exposure from its sector, geography, and business model."},
        {"clause": "4.2",  "title": "Understanding the needs and expectations of interested parties",
         "requirement": "Relevant interested parties (e.g. regulators, investors, employees, clients) and their compliance-related requirements have been identified and documented."},
        {"clause": "4.3",  "title": "Scope of the ABMS",
         "requirement": "The scope of the ABMS is formally documented and reflects the functions, activities, and locations within which it applies."},
        {"clause": "5.1",  "title": "Leadership and commitment",
         "requirement": "Top management demonstrates measurable leadership and commitment: they approve the anti-bribery policy, allocate resources, lead by example, and hold staff accountable."},
        {"clause": "5.2",  "title": "Anti-bribery policy",
         "requirement": "A formal anti-bribery policy has been established, approved by top management, communicated internally and externally as appropriate, and is reviewed regularly."},
        {"clause": "5.3",  "title": "Organizational roles and responsibilities",
         "requirement": "An anti-bribery compliance function or officer has been designated with defined authority and independence to oversee the ABMS."},
        {"clause": "6.1",  "title": "Bribery risk assessment",
         "requirement": "Bribery risks have been formally identified, assessed for likelihood and impact, prioritized, and documented; the risk assessment is reviewed at planned intervals."},
        {"clause": "6.2",  "title": "Anti-bribery objectives",
         "requirement": "Measurable anti-bribery objectives are set, communicated, monitored, and updated when relevant."},
        {"clause": "7.1",  "title": "Resources",
         "requirement": "Adequate financial, human, and technical resources are allocated to operate and maintain the ABMS."},
        {"clause": "7.2",  "title": "Competence and training",
         "requirement": "Personnel have received role-appropriate anti-bribery training; competence is assessed; training records are maintained."},
        {"clause": "7.3",  "title": "Awareness",
         "requirement": "All personnel understand the anti-bribery policy, their contribution to its effectiveness, and the consequences of non-compliance."},
        {"clause": "8.2",  "title": "Due diligence - Financial controls",
         "requirement": "Specific financial controls (e.g. expense approval limits, gift registers, segregation of duties) are implemented and documented to detect and prevent bribery."},
        {"clause": "8.4",  "title": "Gifts, hospitality, donations, and political contributions",
         "requirement": "The organization has a documented policy governing the giving and receiving of gifts, hospitality, and political donations, with defined approval thresholds and registers."},
        {"clause": "8.6",  "title": "Third-party due diligence",
         "requirement": "A formal, risk-based due diligence procedure exists for assessing bribery risk in third-party relationships (agents, JV partners, suppliers) before engagement and periodically thereafter."},
        {"clause": "8.7",  "title": "Raising concerns and whistleblowing",
         "requirement": "A confidential mechanism for raising concerns (including anonymous reporting) is in place; non-retaliation is explicitly guaranteed and communicated."},
        {"clause": "8.8",  "title": "Investigating and dealing with bribery",
         "requirement": "Procedures exist for investigating suspected bribery incidents, reporting outcomes internally and externally as required, and taking corrective action."},
        {"clause": "8.9",  "title": "Business associates - Contractual controls",
         "requirement": "Anti-bribery contractual clauses (representations, warranties, audit rights) are included in contracts with business associates."},
        {"clause": "9.1",  "title": "Monitoring, measurement, analysis, and evaluation",
         "requirement": "ABMS performance is monitored and measured against objectives using defined indicators; results are analysed and reported to top management."},
        {"clause": "9.2",  "title": "Internal audit",
         "requirement": "Internal audits of the ABMS are conducted at planned intervals by competent, independent auditors; results are reported to top management."},
        {"clause": "10.1", "title": "Continual improvement",
         "requirement": "The organization identifies and acts on opportunities to improve the ABMS based on audit findings, non-conformities, and changing bribery risks."},
    ],

    # -- ISO 37002:2021  Whistleblowing Management Systems -------------------
    "iso37002": [
        {"clause": "4.1",  "title": "Understanding the organization and its context",
         "requirement": "The organization has identified internal and external factors that affect its whistleblowing management system (WMS) and the ability of stakeholders to raise concerns."},
        {"clause": "4.2",  "title": "Interested parties",
         "requirement": "Parties who need or are expected to use the whistleblowing system (employees, contractors, suppliers, the public) have been identified and their needs assessed."},
        {"clause": "4.3",  "title": "Scope of the WMS",
         "requirement": "The scope of the WMS - which concerns it covers and who may report - is formally defined and documented."},
        {"clause": "5.1",  "title": "Leadership commitment to the WMS",
         "requirement": "Top management publicly commits to a speak-up culture; they allocate resources to the WMS and ensure non-retaliation is actively enforced."},
        {"clause": "5.2",  "title": "Whistleblowing policy",
         "requirement": "A formal, accessible whistleblowing policy is in place, approved by top management, and communicated to all potential reporters."},
        {"clause": "5.3",  "title": "Roles and responsibilities",
         "requirement": "Roles for receiving, assessing, and investigating concerns are clearly defined and assigned to competent individuals with appropriate independence."},
        {"clause": "6.1",  "title": "Actions to address risks",
         "requirement": "Risks to the effectiveness of the WMS (e.g. under-reporting, retaliation, conflict of interest) have been identified and mitigation measures documented."},
        {"clause": "7.2",  "title": "Competence of WMS personnel",
         "requirement": "Personnel handling reports are trained in investigation techniques, confidentiality obligations, and applicable legal requirements."},
        {"clause": "7.4",  "title": "Communication and reporting channels",
         "requirement": "Multiple accessible reporting channels (phone hotline, web portal, in-person) exist; anonymous reporting is supported; channel details are widely communicated."},
        {"clause": "8.1",  "title": "Receiving concerns",
         "requirement": "A documented procedure governs how concerns are received, acknowledged (within a defined timeframe), and recorded."},
        {"clause": "8.2",  "title": "Assessing concerns",
         "requirement": "Received concerns are formally assessed for credibility and scope by a competent, independent party before investigation is initiated."},
        {"clause": "8.3",  "title": "Investigating concerns",
         "requirement": "Investigations follow a documented procedure, are conducted by trained investigators, are proportionate to severity, and are completed within a defined timeframe."},
        {"clause": "8.4",  "title": "Concluding the investigation",
         "requirement": "Investigation conclusions are formally documented; outcomes are communicated to the reporter (to the extent possible) and relevant decision-makers."},
        {"clause": "8.5",  "title": "Protecting whistleblowers",
         "requirement": "Explicit, enforceable non-retaliation protections are in place; retaliation complaints are handled through a separate, rapid process."},
        {"clause": "8.6",  "title": "Protection of personal data",
         "requirement": "Personal data of whistleblowers and persons subject to investigation is handled in accordance with applicable data protection laws and the organization's privacy policy."},
        {"clause": "8.7",  "title": "Remediation and corrective action",
         "requirement": "Outcomes of substantiated concerns lead to documented corrective actions with assigned owners and follow-up to verify effectiveness."},
        {"clause": "9.1",  "title": "Monitoring and evaluation of the WMS",
         "requirement": "WMS performance is tracked using metrics (e.g. volume of reports, resolution time, retaliation incidents); results are reviewed by top management."},
        {"clause": "10.1", "title": "Continual improvement",
         "requirement": "Trends in whistleblowing data are used to drive systemic improvements; the WMS is reviewed after significant incidents."},
    ],

    # -- ISO 37301:2021  Compliance Management Systems -----------------------
    "iso37301": [
        {"clause": "4.1",  "title": "Understanding the organization and its context",
         "requirement": "The organization has identified external and internal factors that affect its ability to achieve compliance obligations and manage compliance risks."},
        {"clause": "4.2",  "title": "Interested parties",
         "requirement": "Parties with a stake in the compliance management system (regulators, shareholders, employees) and their requirements have been identified."},
        {"clause": "4.4",  "title": "Compliance obligations",
         "requirement": "All applicable legal, regulatory, and voluntary compliance obligations have been identified, documented, and assigned to owners."},
        {"clause": "5.1",  "title": "Governing body and top management oversight",
         "requirement": "The governing body and top management provide active, visible oversight of the compliance management system; a culture of compliance is promoted."},
        {"clause": "5.2",  "title": "Compliance policy",
         "requirement": "A formal compliance policy is established, approved, communicated organization-wide, and reviewed at planned intervals."},
        {"clause": "5.3",  "title": "Compliance function",
         "requirement": "A designated compliance function with sufficient authority, resources, and independence has been established."},
        {"clause": "6.1",  "title": "Compliance risk assessment",
         "requirement": "Compliance risks are systematically identified, assessed for impact and likelihood, prioritized, and documented; assessment is updated when significant changes occur."},
        {"clause": "6.2",  "title": "Compliance objectives",
         "requirement": "Measurable compliance objectives are set at relevant functions and levels and are monitored."},
        {"clause": "7.2",  "title": "Competence",
         "requirement": "All personnel have the competence required for their compliance-related responsibilities; training gaps are identified and addressed."},
        {"clause": "7.3",  "title": "Awareness",
         "requirement": "Personnel are aware of the compliance policy, their obligations, and the consequences of non-compliance."},
        {"clause": "7.4",  "title": "Communication",
         "requirement": "Internal and external communication about compliance matters follows a documented plan."},
        {"clause": "8.1",  "title": "Operational planning and controls",
         "requirement": "Documented controls address each identified compliance obligation; controls are implemented consistently across the scope."},
        {"clause": "8.3",  "title": "Third-party relationships",
         "requirement": "Compliance requirements are extended to significant third parties through due diligence, contractual obligations, and monitoring."},
        {"clause": "9.1",  "title": "Monitoring, measurement, analysis, and evaluation",
         "requirement": "The effectiveness of the compliance management system is monitored using defined indicators; results are periodically reported to top management."},
        {"clause": "9.2",  "title": "Internal audit",
         "requirement": "Independent internal audits of the compliance management system are conducted and findings are reported to the governing body."},
        {"clause": "10.1", "title": "Non-compliance and corrective action",
         "requirement": "Non-compliance incidents are identified, investigated, root-caused, remediated, and documented; recurrence is prevented."},
        {"clause": "10.2", "title": "Continual improvement",
         "requirement": "The organization systematically improves the compliance management system based on audit outcomes, non-conformities, and performance data."},
    ],

    # -- ISO 37000:2021  Governance of Organizations -------------------------
    "iso37000": [
        {"clause": "5.1",  "title": "Purpose of the organization",
         "requirement": "The governing body has defined and formally adopted the organization's purpose, and ensures decisions align with that purpose."},
        {"clause": "5.2",  "title": "Values and ethical culture",
         "requirement": "The governing body defines the organization's values, promotes an ethical culture, and models the desired behaviour."},
        {"clause": "5.3",  "title": "Strategy",
         "requirement": "Strategy is set by the governing body, communicated clearly, and subject to regular review against the organization's purpose."},
        {"clause": "6.1",  "title": "Composition and structure of the governing body",
         "requirement": "The governing body has an appropriate composition (skills, diversity, independence) and a clear internal structure with defined roles."},
        {"clause": "6.2",  "title": "Decision-making",
         "requirement": "Decision-making processes are documented, transparent, and accountable; conflict-of-interest procedures are in place and followed."},
        {"clause": "6.3",  "title": "Policies and delegated authority",
         "requirement": "The governing body has approved key policies and clearly delegated authority to management, with retained oversight."},
        {"clause": "7.1",  "title": "Accountability",
         "requirement": "Accountability mechanisms are in place: the governing body holds management to account, and is itself accountable to owners/stakeholders."},
        {"clause": "7.2",  "title": "Transparency and disclosure",
         "requirement": "Material information is disclosed to stakeholders accurately, completely, and in a timely manner."},
        {"clause": "7.3",  "title": "Responsible stewardship",
         "requirement": "The governing body exercises responsible stewardship of organizational resources, considering long-term sustainability."},
        {"clause": "8.1",  "title": "Risk oversight",
         "requirement": "The governing body provides oversight of the organization's risk management framework and appetite."},
        {"clause": "8.2",  "title": "Organizational performance",
         "requirement": "The governing body monitors organizational performance against strategy using financial and non-financial measures."},
        {"clause": "8.3",  "title": "Compliance and integrity oversight",
         "requirement": "The governing body oversees compliance management and ethical conduct across the organization."},
        {"clause": "9.1",  "title": "Stakeholder engagement",
         "requirement": "The governing body has a formal approach to stakeholder identification and engagement that influences organizational decisions."},
        {"clause": "9.2",  "title": "Evaluation of governing body effectiveness",
         "requirement": "The effectiveness of the governing body and its individual members is regularly and independently evaluated."},
    ],
}

# -- Pillar -> Clause mapping (for radar chart) --------------------------------
PILLAR_CLAUSE_MAP = {
    "Governance":          ["4.1", "4.2", "4.3", "5.1", "5.2", "5.3"],
    "Risk Assessment":     ["6.1", "6.2"],
    "Due Diligence":       ["8.6", "8.3"],
    "Financial Controls":  ["8.2", "8.4"],
    "Reporting Channels":  ["7.4", "8.1"],
    "Investigation":       ["8.2", "8.3", "8.4", "8.5", "8.7", "8.8"],
    "Whistleblowing":      ["8.7", "8.9", "5.2"],
    "Training & Awareness": ["7.2", "7.3"],
    "Monitoring":          ["9.1", "9.2", "10.1", "10.2"],
    "Controls":            ["8.1", "8.9"],
}

ISO_NAME_MAP = {
    "iso37001": "ISO 37001 (Anti-bribery Management Systems)",
    "iso37002": "ISO 37002 (Whistleblowing Management Systems)",
    "iso37301": "ISO 37301 (Compliance Management Systems)",
    "iso37000": "ISO 37000 (Governance of Organizations)",
}


# =============================================================================
#  SECTION 1: Company Document Parsing
# =============================================================================

def _extract_section_heading(text_block: str) -> str:
    lines = text_block.strip().split("\n")
    for line in lines[:6]:
        line = line.strip()
        if 10 < len(line) < 120 and (
            line.istitle()
            or line.isupper()
            or re.match(r"^[\d]+[\.\d]*\s+[A-Z]", line)
        ):
            return line
    return ""


def extract_company_pages(file_paths: list[str]) -> list[dict]:
    pages = []
    for fp in file_paths:
        doc_name = os.path.splitext(os.path.basename(fp))[0].replace("_", " ").replace("-", " ")
        try:
            with pdfplumber.open(fp) as pdf:
                for page_no, page in enumerate(pdf.pages, start=1):
                    text = (page.extract_text() or "").strip()
                    if len(text) < 30:
                        continue
                    heading = _extract_section_heading(text)
                    pages.append({
                        "filename":        os.path.basename(fp),
                        "doc_name":        doc_name,
                        "page_no":         page_no,
                        "section_heading": heading,
                        "text":            text,
                    })
        except Exception as exc:
            print(f"  Warning: could not parse {fp}: {exc}")
    return pages


def _build_company_context_block(pages: list[dict], max_chars: int = 250000) -> str:
    lines, total = [], 0
    for p in pages:
        heading_part = f" | Section: \"{p['section_heading']}\"" if p["section_heading"] else ""
        header = f"[DOC: {p['doc_name']} | Page {p['page_no']}{heading_part}]"
        block  = f"{header}\n{p['text']}\n"
        if total + len(block) > max_chars:
            break
        lines.append(block)
        total += len(block)
    return "\n".join(lines)


# =============================================================================
#  SECTION 2: ISO Knowledge Base Retrieval (concurrent async)
# =============================================================================

# Module-level ChromaDB client - created once, reused across all requests
_chroma_client     = chromadb.PersistentClient(path=CHROMA_DIR)
_chroma_collection = None


def _get_collection():
    global _chroma_collection
    if _chroma_collection is None:
        _chroma_collection = _chroma_client.get_collection(
            COLLECTION_NAME, embedding_function=_ef
        )
    return _chroma_collection


def _retrieve_iso_context_sync(requirement: str, iso_name: str, n_results: int = 10) -> str:
    """Synchronous ChromaDB lookup with metadata filtering by ISO framework."""
    try:
        col     = _get_collection()
        results = col.query(
            query_texts=[requirement],
            n_results=n_results,
            where={"source": iso_name}
        )
        docs    = results.get("documents", [[]])[0]
        metas   = results.get("metadatas", [[]])[0]
        return "\n\n".join(
            f"[ISO Source: {m.get('source','?')}, Page {m.get('page','?')}]\n{d}"
            for d, m in zip(docs, metas)
        )
    except Exception as exc:
        print(f"  ChromaDB warning: {exc}")
        return ""


async def _retrieve_all_contexts(checklist: list[dict], iso_name: str) -> list[str]:
    """Retrieve ChromaDB context for ALL clauses concurrently, filtered by framework."""
    loop  = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(None, _retrieve_iso_context_sync, item["requirement"], iso_name)
        for item in checklist
    ]
    return await asyncio.gather(*tasks)


# =============================================================================
#  SECTION 3: BATCH Gemini Scoring (single API call for all clauses)
# =============================================================================

BATCH_PROMPT_TEMPLATE = """
You are a Senior Compliance Auditor at Deloitte assessing ISO compliance.
Evaluate EVERY clause in the CHECKLIST below against the company documents.
Return a valid JSON ARRAY - one object per clause, in the same order as the checklist.

## Standard
{iso_name}

## Company Documents (page-labelled for citation)
{company_context}

## ISO Knowledge Base Excerpts
{iso_context_block}

## CHECKLIST (evaluate all {n} clauses)
{checklist_block}

## Output format (JSON array, one object per clause, same order as checklist):
[
  {{
    "clause": "<clause number>",
    "status": "Compliant" | "Partially Compliant" | "Non-Compliant",
    "score": <0-100>,
    "iso_reference": {{
      "standard": "<standard name>",
      "clause": "<clause number>",
      "clause_title": "<title>"
    }},
    "company_citation": {{
      "found": <true|false>,
      "document": "<filename or N/A>",
      "page_no": <integer or null>,
      "section_heading": "<heading or ''>",
      "verbatim_excerpt": "<exact quote max 60 words or ''>"
    }},
    "assessment_summary": "<2-3 sentence professional finding>",
    "gap": "<gap description or ''>",
    "recommendation": "<actionable step or ''>"
  }}
]

Rules:
- Return ONLY the JSON array. No markdown fences, no preamble, no explanation.
- Every clause must have an entry. Do not skip any clause.
- verbatim_excerpt must be copied exactly from the company documents above.
- assessment_summary must reference the specific evidence (or absence of it).
"""


def _build_checklist_block(checklist: list[dict], iso_contexts: list[str]) -> str:
    lines = []
    for i, (item, ctx) in enumerate(zip(checklist, iso_contexts), 1):
        lines.append(
            f"[{i}] Clause {item['clause']}: {item['title']}\n"
            f"    Requirement: {item['requirement']}\n"
            f"    ISO Reference: {ctx[:400] if ctx else 'See knowledge base'}\n"
        )
    return "\n".join(lines)


def _parse_strip_json(text: str) -> list:
    """Robustly extract a JSON array from Gemini's response, even with fences/preamble."""
    text = text.strip()

    # 1. Strip markdown fences
    text = re.sub(r"^```(?:json)*", "", text, flags=re.MULTILINE|re.IGNORECASE)
    text = re.sub(r"```$", "", text, flags=re.MULTILINE).strip()

    # 2. Try direct parse
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # 3. Find the first '[' ... last ']' and try to parse that
    start = text.find("[")
    end   = text.rfind("]")
    if start != -1 and end > start:
        try:
            result = json.loads(text[start:end+1])
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not extract JSON array from response (first 300 chars): {text[:300]}")


def _extract_retry_delay(error_str: str, default: float = 30.0) -> float:
    """Parse 'retry_delay { seconds: 15 }' from a 429 error string."""
    m = re.search(r"retry_delay\s*\{[^}]*seconds:\s*(\d+)", error_str)
    return float(m.group(1)) + 2.0 if m else default


def _is_quota_error(exc: Exception) -> bool:
    return "429" in str(exc) or "quota" in str(exc).lower() or "RESOURCE_EXHAUSTED" in str(exc)


def _score_chunk(
    iso_name: str,
    chunk_items: list[dict],
    chunk_contexts: list[str],
    company_context: str,
    chunk_num: int,
    max_retries: int = 4,
) -> list[dict]:
    """Score a small chunk of clauses. Auto-retries on 429 with the delay Gemini specifies."""
    checklist_block = _build_checklist_block(chunk_items, chunk_contexts)
    ctx_trimmed     = company_context[:250000]
    iso_parts       = [c for c in chunk_contexts if c]
    iso_block       = "\n---\n".join(iso_parts[:6]) or "Not available — assess from company documents."

    prompt = BATCH_PROMPT_TEMPLATE.format(
        iso_name          = iso_name,
        company_context   = ctx_trimmed,
        iso_context_block = iso_block,
        checklist_block   = checklist_block,
        n                 = len(chunk_items),
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = llm.generate_content(prompt)
            raw_text = response.text
            print(f"  [Chunk {chunk_num}] {len(raw_text)} chars via {GEMINI_MODEL} (attempt {attempt})")
            results  = _parse_strip_json(raw_text)
            if isinstance(results, list):
                print(f"  [Chunk {chunk_num}] Parsed {len(results)}/{len(chunk_items)} results OK")
                return results
            raise ValueError(f"Not a list: {type(results)}")

        except Exception as exc:
            err_str = str(exc)
            if _is_quota_error(exc) and attempt < max_retries:
                wait = _extract_retry_delay(err_str, default=30.0 * attempt)
                print(f"  [Chunk {chunk_num}] Rate-limited. Waiting {wait:.0f}s then retry {attempt+1}/{max_retries}...")
                time.sleep(wait)
                continue
            print(f"  [Chunk {chunk_num}] FAILED after {attempt} attempt(s): {err_str[:200]}")
            return [_error_finding(item, "Quota exceeded — please retry later.") for item in chunk_items]

    return [_error_finding(i, "Max retries exceeded") for i in chunk_items]


def _batch_score_all_clauses(
    iso_name: str,
    checklist: list[dict],
    iso_contexts: list[str],
    company_context: str,
) -> list[dict]:
    """
    Score all clauses in sequential chunks of CHUNK_SIZE with rate-limit awareness.
    Adds a short inter-chunk pause to stay within free-tier RPM limits.
    """
    all_results = []
    chunks = [
        (checklist[i:i+CHUNK_SIZE], iso_contexts[i:i+CHUNK_SIZE])
        for i in range(0, len(checklist), CHUNK_SIZE)
    ]
    print(f"  Scoring {len(checklist)} clauses in {len(chunks)} chunks of {CHUNK_SIZE}...")
    for k, (chunk_items, chunk_ctxs) in enumerate(chunks, 1):
        if k > 1:
            time.sleep(4)   # 4s between chunks keeps us under 15 RPM free tier
        results = _score_chunk(iso_name, chunk_items, chunk_ctxs, company_context, k)
        while len(results) < len(chunk_items):
            results.append(_error_finding(chunk_items[len(results)], "Missing from response"))
        all_results.extend(results[:len(chunk_items)])
    return all_results



def _error_finding(clause_item: dict, reason: str) -> dict:
    return {
        "clause":  clause_item.get("clause", "?"),
        "status":  "Error",
        "score":   0,
        "iso_reference": {
            "standard":     "",
            "clause":       clause_item.get("clause", "?"),
            "clause_title": clause_item.get("title", "?"),
        },
        "company_citation": {
            "found": False, "document": "N/A",
            "page_no": None, "section_heading": "", "verbatim_excerpt": "",
        },
        "assessment_summary": reason,
        "gap":            reason,
        "recommendation": "Retry assessment.",
    }


# =============================================================================
#  SECTION 4: Aggregation & Main Pipeline
# =============================================================================

def _compute_pillar_scores(findings: list[dict]) -> dict:
    scores = {}
    for pillar, clauses in PILLAR_CLAUSE_MAP.items():
        relevant = [f for f in findings if any(f["clause"].startswith(c) for c in clauses)]
        scores[pillar] = (
            round(sum(f["score"] for f in relevant) / len(relevant))
            if relevant else 75
        )
    return scores


async def process_documents_and_score(
    job_id: str,
    framework: str,
    file_paths: list[str],
) -> dict:
    """
    Optimised orchestrator:
    1. Extract company docs (once)
    2. Retrieve all ISO contexts concurrently via asyncio.gather
    3. ONE batch Gemini call evaluates all clauses - major speed gain
    4. Aggregate scores
    """
    t0 = time.time()
    print(f"\n[Job {job_id}] Starting {framework} assessment...")

    checklist = ISO_CHECKLISTS.get(framework, [])
    if not checklist:
        return {"error": f"Unknown framework: {framework}"}

    iso_name = ISO_NAME_MAP.get(framework, framework.upper())

    # Step 1: Parse company documents (once)
    print("  [1/3] Parsing company documents...")
    company_pages   = extract_company_pages(file_paths)
    company_context = _build_company_context_block(company_pages)
    print(f"        -> {len(company_pages)} pages from {len(file_paths)} file(s)")

    # Step 2: Retrieve ISO contexts concurrently for the correct framework
    print(f"  [2/3] Retrieving ISO context for {len(checklist)} clauses (parallel)...")
    iso_contexts = await _retrieve_all_contexts(checklist, iso_name)
    print(f"        -> Done in {time.time()-t0:.1f}s")

    # Step 3: Single batch Gemini call
    print(f"  [3/3] Batch scoring all {len(checklist)} clauses in ONE Gemini call...")
    raw_results = _batch_score_all_clauses(iso_name, checklist, iso_contexts, company_context)

    # Build findings
    findings = []
    for item, result in zip(checklist, raw_results):
        iso_ref = result.get("iso_reference", {})
        # Force the standard name to prevent any LLM hallucination of other frameworks
        iso_ref["standard"] = iso_name
        if "clause" not in iso_ref: iso_ref["clause"] = item["clause"]
        if "clause_title" not in iso_ref: iso_ref["clause_title"] = item["title"]

        finding = {
            "clause":       item["clause"],
            "clause_title": item["title"],
            "requirement":  item["requirement"],
            "status":       result.get("status", "Error"),
            "score":        result.get("score", 0),
            "iso_reference": iso_ref,
            "company_citation": result.get("company_citation", {
                "found": False, "document": "N/A", "page_no": None,
                "section_heading": "", "verbatim_excerpt": ""
            }),
            "assessment_summary": result.get("assessment_summary", ""),
            "gap":               result.get("gap", ""),
            "recommendation":    result.get("recommendation", ""),
        }
        findings.append(finding)
        cite = finding["company_citation"]
        loc  = f"{cite.get('document','?')} p.{cite.get('page_no','?')}" \
               if cite.get("found") else "no citation"
        print(f"    Clause {item['clause']}: {finding['status']} ({finding['score']}%) | {loc}")

    # Aggregate
    overall = round(sum(f["score"] for f in findings) / len(findings)) if findings else 0
    pillars = _compute_pillar_scores(findings)

    # Cleanup temp files
    for fp in file_paths:
        if os.path.exists(fp):
            try:
                os.remove(fp)
            except Exception:
                pass

    elapsed = time.time() - t0
    payload = {
        "job_id":          job_id,
        "status":          "completed",
        "framework":       framework,
        "framework_name":  iso_name,
        "overall_score":   overall,
        "pillars":         pillars,
        "findings":        findings,
        "total_clauses":   len(findings),
        "compliant":       sum(1 for f in findings if f["status"] == "Compliant"),
        "partial":         sum(1 for f in findings if f["status"] == "Partially Compliant"),
        "non_compliant":   sum(1 for f in findings if f["status"] == "Non-Compliant"),
        "elapsed_seconds": round(elapsed),
    }
    print(f"[Job {job_id}] DONE in {elapsed:.1f}s | Score: {overall}% | "
          f"C:{payload['compliant']} P:{payload['partial']} NC:{payload['non_compliant']}\n")
    return payload
