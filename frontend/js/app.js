/**
 * app.js  –  Anupalan Mitra (Big4 Deliverable Style)
 * Vanilla JS only, no ES modules (works via file:// and http://).
 */

const BASE_URL      = "http://localhost:8000/api/v1";
const POLL_INTERVAL = 3000;
const MAX_POLLS     = 120; // 6 min max

// ═══════════════════════════════════════════════════════════════════
//  INLINE API
// ═══════════════════════════════════════════════════════════════════
async function apiStartAssessment(framework, files) {
    const fd = new FormData();
    fd.append("framework", framework);
    files.forEach(f => fd.append("files", f));
    const res = await fetch(`${BASE_URL}/assess`, { method: "POST", body: fd });
    if (!res.ok) { const e = await res.json(); throw new Error(e.detail || "Upload failed"); }
    return res.json();
}

async function apiPollResults(jobId, onProgress) {
    for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise(r => setTimeout(r, POLL_INTERVAL));
        const res  = await fetch(`${BASE_URL}/results/${jobId}`);
        if (!res.ok) throw new Error("Failed to fetch results");
        const data = await res.json();
        if (data.status === "completed") return data;
        if (data.status === "not_found")  throw new Error("Job not found. Please retry.");
        if (onProgress) onProgress(i);
    }
    throw new Error("Timed out — please retry.");
}

// ═══════════════════════════════════════════════════════════════════
//  MOCK DATA (fallback / demo)
// ═══════════════════════════════════════════════════════════════════
function getMockData(fw) {
    const names = {
        iso37001: "ISO 37001 – Anti-bribery Management Systems",
        iso37002: "ISO 37002 – Whistleblowing Management Systems",
        iso37301: "ISO 37301 – Compliance Management Systems",
        iso37000: "ISO 37000 – Governance of Organizations",
    };
    return {
        framework: fw, framework_name: names[fw] || "ISO Standard",
        status: "completed", overall_score: 74, total_clauses: 5,
        compliant: 2, partial: 2, non_compliant: 1, elapsed_seconds: 45,
        pillars: { Governance: 88, "Risk Assessment": 72, "Due Diligence": 58, "Financial Controls": 80, Whistleblowing: 65 },
        findings: [
            {
                clause: "5.2", clause_title: "Anti-bribery policy", status: "Compliant", score: 95,
                iso_reference: { standard: "ISO 37001", clause: "5.2", clause_title: "Anti-bribery policy" },
                company_citation: { found: true, document: "Tata_Anti_Bribery_Policy.pdf", page_no: 3, section_heading: "Policy Statement", verbatim_excerpt: "Tata has a zero-tolerance approach to bribery and corruption. All employees, directors, and third-party representatives are required to comply with this policy." },
                assessment_summary: "A clear zero-tolerance anti-bribery policy statement has been formally identified across the uploaded documents, satisfying Clause 5.2 in full.",
                gap: "", recommendation: "",
            },
            {
                clause: "4.1", clause_title: "Understanding organizational context", status: "Compliant", score: 88,
                iso_reference: { standard: "ISO 37001", clause: "4.1", clause_title: "Understanding the organization and its context" },
                company_citation: { found: true, document: "Governance_Report.pdf", page_no: 12, section_heading: "Enterprise Risk Management", verbatim_excerpt: "The Board periodically reviews the risk management framework to identify ethical, regulatory, and geopolitical risks that may impact the organization." },
                assessment_summary: "The governance report addresses both internal and external risk factors, satisfying the context analysis requirement.",
                gap: "", recommendation: "",
            },
            {
                clause: "8.6", clause_title: "Third-party due diligence", status: "Partially Compliant", score: 58,
                iso_reference: { standard: "ISO 37001", clause: "8.6", clause_title: "Third-party due diligence" },
                company_citation: { found: true, document: "Code_of_Conduct.pdf", page_no: 18, section_heading: "Supplier Relationships", verbatim_excerpt: "Tata companies are expected to conduct business only with reputable third parties. Appropriate screening should be applied before new relationships." },
                assessment_summary: "While the Code references third-party screening, no formal risk-scoring matrix or re-assessment cadence exists as required by Clause 8.6.",
                gap: "No formal bribery-specific due diligence procedure with risk tiers and escalation workflow.",
                recommendation: "Establish a Third-Party Anti-Bribery Due Diligence SOP covering risk scoring, approval tiers, and periodic re-assessment at least annually.",
            },
            {
                clause: "7.2", clause_title: "Competence and training", status: "Partially Compliant", score: 62,
                iso_reference: { standard: "ISO 37001", clause: "7.2", clause_title: "Competence and training" },
                company_citation: { found: true, document: "Tata_Anti_Bribery_Policy.pdf", page_no: 7, section_heading: "Training Requirements", verbatim_excerpt: "All employees are required to complete anti-bribery training as part of the onboarding process." },
                assessment_summary: "Onboarding training exists but no ongoing periodic training schedule or competence assessment records were evidenced across the documents.",
                gap: "Absence of periodic refresher training cadence and competence assessment records.",
                recommendation: "Implement annual anti-bribery training with assessment logs maintained per employee, mapped to role-specific bribery exposure.",
            },
            {
                clause: "8.9", clause_title: "Whistleblowing / Raising concerns", status: "Non-Compliant", score: 35,
                iso_reference: { standard: "ISO 37001", clause: "8.9", clause_title: "Whistleblowing / Raising concerns" },
                company_citation: { found: false, document: "N/A", page_no: null, section_heading: "", verbatim_excerpt: "" },
                assessment_summary: "No clause within the reviewed documents explicitly guarantees non-retaliation or references a designated anonymous whistleblowing channel for bribery concerns.",
                gap: "Non-retaliation guarantee absent from all anti-bribery-specific documents; no anonymous channel referenced.",
                recommendation: "Amend the Anti-Bribery Policy to cite the Whistleblower Policy and designate an anonymous reporting channel with an explicit non-retaliation guarantee.",
            },
        ]
    };
}

// ═══════════════════════════════════════════════════════════════════
//  UPLOAD PAGE
// ═══════════════════════════════════════════════════════════════════
function initUploadPage() {
    const dropZone    = document.getElementById("dropZone");
    const fileInput   = document.getElementById("fileInput");
    const runBtn      = document.getElementById("runBtn");
    const uploadCard  = document.getElementById("uploadCard");
    const loadingState= document.getElementById("loadingState");
    const loadingMsg  = document.getElementById("loadingMsg");

    const LOADING_STEPS = ["ls1","ls2","ls3","ls4"];
    let uploadedFiles = [];

    // ── Drag-and-drop ──
    ["dragenter","dragover","dragleave","drop"].forEach(ev =>
        dropZone.addEventListener(ev, e => { e.preventDefault(); e.stopPropagation(); }));
    ["dragenter","dragover"].forEach(ev => dropZone.addEventListener(ev, () => dropZone.classList.add("drag-over")));
    ["dragleave","drop"].forEach(ev => dropZone.addEventListener(ev, () => dropZone.classList.remove("drag-over")));
    dropZone.addEventListener("drop",    e => addFiles(e.dataTransfer.files));
    fileInput.addEventListener("change", function() { addFiles(this.files); });

    function addFiles(list) {
        const newFiles = Array.from(list).filter(f =>
            !uploadedFiles.some(u => u.name === f.name && u.size === f.size)
        );
        uploadedFiles = [...uploadedFiles, ...newFiles].slice(0, 4); // max 4
        renderSlots();
        validate();
    }

    function renderSlots() {
        for (let i = 0; i < 4; i++) {
            const slot = document.getElementById("slot" + i);
            if (!slot) continue;
            const file = uploadedFiles[i];
            if (file) {
                slot.className = "doc-slot filled";
                slot.innerHTML = `
                    <span class="slot-icon">📄</span>
                    <div style="flex:1;min-width:0">
                        <div class="slot-file" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${file.name}">${file.name}</div>
                        <div class="slot-label">${(file.size/1048576).toFixed(2)} MB</div>
                    </div>
                    <button class="slot-remove" data-i="${i}" title="Remove">✕</button>`;
            } else {
                slot.className = "doc-slot";
                slot.innerHTML = `<span class="slot-icon">📄</span><span class="slot-label">Document ${i+1}</span>`;
            }
        }
        document.querySelectorAll(".slot-remove").forEach(btn => {
            btn.addEventListener("click", function(e) {
                e.stopPropagation();
                uploadedFiles.splice(+this.dataset.i, 1);
                renderSlots(); validate();
            });
        });
    }

    function validate() {
        const fw = document.querySelector("input[name='framework']:checked");
        runBtn.disabled = !(fw && uploadedFiles.length > 0);
    }

    document.querySelectorAll("input[name='framework']").forEach(r =>
        r.addEventListener("change", validate));

    function setLoadingStep(idx) {
        LOADING_STEPS.forEach((id, i) => {
            const el = document.getElementById(id);
            if (!el) return;
            el.className = "lstep " + (i < idx ? "done" : i === idx ? "active" : "");
            if (i < idx) el.querySelector(".lstep-dot").innerHTML = "✓";
        });
    }

    runBtn.addEventListener("click", async () => {
        const fw = document.querySelector("input[name='framework']:checked");
        if (!fw || uploadedFiles.length === 0) return;

        // Show loading
        runBtn.style.display    = "none";
        loadingState.style.display = "flex";
        setLoadingStep(0);

        const framework = fw.value;
        try {
            if (loadingMsg) loadingMsg.textContent = "Uploading " + uploadedFiles.length + " document(s)…";
            setLoadingStep(0);
            const { job_id } = await apiStartAssessment(framework, uploadedFiles);
            sessionStorage.setItem("job_id",    job_id);
            sessionStorage.setItem("framework", framework);

            setLoadingStep(1);
            if (loadingMsg) loadingMsg.textContent = "Parsing documents & retrieving ISO clauses…";

            await apiPollResults(job_id, i => {
                if (i < 5)  { setLoadingStep(1); if (loadingMsg) loadingMsg.textContent = "Parsing & retrieving ISO context…"; }
                if (i >= 5) { setLoadingStep(2); if (loadingMsg) loadingMsg.textContent = "Gemini AI batch scoring all " + uploadedFiles.length + " docs…"; }
                if (i >= 15){ setLoadingStep(3); if (loadingMsg) loadingMsg.textContent = "Building gap register & evidence map…"; }
            });

            setLoadingStep(4);
            window.location.href = "dashboard.html";
        } catch (err) {
            console.warn("Backend unreachable – launching demo mode:", err.message);
            sessionStorage.removeItem("job_id");
            sessionStorage.setItem("demo_mode", "true");
            sessionStorage.setItem("framework", framework);
            setLoadingStep(3);
            if (loadingMsg) loadingMsg.textContent = "Demo mode – showing sample report…";
            setTimeout(() => { window.location.href = "dashboard.html"; }, 1500);
        }
    });
}

// ═══════════════════════════════════════════════════════════════════
//  DASHBOARD PAGE
// ═══════════════════════════════════════════════════════════════════
async function initDashboard() {
    const jobId     = sessionStorage.getItem("job_id");
    const demo      = sessionStorage.getItem("demo_mode") === "true";
    const framework = sessionStorage.getItem("framework") || "iso37001";
    let data;

    if (demo || !jobId) {
        data = getMockData(framework);
    } else {
        try {
            const res  = await fetch(`${BASE_URL}/results/${jobId}`);
            const json = await res.json();
            data = (json.status === "completed") ? json : getMockData(framework);
        } catch {
            data = getMockData(framework);
        }
    }

    document.getElementById("dashLoading").style.display = "none";
    document.getElementById("dashContent").style.display = "block";
    renderDashboard(data);
}

function renderDashboard(data) {
    if (!data) return;
    const score    = data.overall_score ?? 0;
    const findings = data.findings || [];

    // ── Header ──
    document.getElementById("rptTitle").textContent   = data.framework_name || "ISO Compliance Report";
    document.getElementById("rptSubtitle").textContent= "Consolidated assessment across all uploaded documents";
    document.getElementById("rptDate").textContent    = new Date().toLocaleDateString("en-IN", { day:"numeric", month:"long", year:"numeric" });
    document.getElementById("rptClauses").textContent = data.total_clauses ?? findings.length;
    document.getElementById("rptTime").textContent    = data.elapsed_seconds ? data.elapsed_seconds + "s" : "—";

    // ── Scorecard ──
    const scoreEl = document.getElementById("overallScore");
    scoreEl.textContent = score + "%";
    scoreEl.style.color = score >= 80 ? "#22c55e" : score >= 60 ? "#f59e0b" : "#ef4444";

    const lvEl = document.getElementById("scoreLevel");
    if (score >= 80) { lvEl.textContent = "🟢 Advanced";      lvEl.className = "score-level level-advanced"; }
    else if (score >= 60) { lvEl.textContent = "🟡 Intermediate"; lvEl.className = "score-level level-intermediate"; }
    else              { lvEl.textContent = "🔴 Foundational";  lvEl.className = "score-level level-foundational"; }

    document.getElementById("statCompliant").textContent = data.compliant ?? 0;
    document.getElementById("statPartial").textContent   = data.partial   ?? 0;
    document.getElementById("statNC").textContent        = data.non_compliant ?? 0;
    document.getElementById("statTotal").textContent     = data.total_clauses ?? findings.length;

    // ── Radar chart ──
    if (data.pillars && typeof Chart !== "undefined") {
        const canvas = document.getElementById("radarChart");
        Chart.defaults.color       = "#64748b";
        Chart.defaults.font.family = "'Inter', sans-serif";
        new Chart(canvas, {
            type: "radar",
            data: {
                labels:   Object.keys(data.pillars),
                datasets: [{
                    label: "Readiness %",
                    data:  Object.values(data.pillars),
                    backgroundColor:      "rgba(59,130,246,0.18)",
                    borderColor:          "#3b82f6",
                    pointBackgroundColor: "#8b5cf6",
                    pointBorderColor:     "#fff",
                    borderWidth: 2,
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: { r: {
                    min: 0, max: 100,
                    angleLines: { color: "rgba(255,255,255,0.07)" },
                    grid:        { color: "rgba(255,255,255,0.07)" },
                    ticks:       { display: false },
                    pointLabels: { font: { size: 11, weight: 600 }, color: "#94a3b8" },
                }},
                plugins: { legend: { display: false } }
            }
        });
    }

    // ── Risk Heatmap (sorted by score asc = highest risk first) ──
    const heatGrid = document.getElementById("heatmapGrid");
    if (heatGrid) {
        const sorted = [...findings].sort((a,b) => a.score - b.score);
        sorted.forEach(f => {
            const pct  = f.score ?? 0;
            const cls  = f.status === "Compliant" ? "hc" : f.status === "Partially Compliant" ? "hp" : "hn";
            const col  = f.status === "Compliant" ? "#22c55e" : f.status === "Partially Compliant" ? "#f59e0b" : "#ef4444";
            const bdge = f.status === "Compliant" ? "✓ Compliant" : f.status === "Partially Compliant" ? "⚠ Partial" : "✕ Non-Compliant";
            const div  = document.createElement("div");
            div.className = `heatmap-item ${cls}`;
            div.innerHTML = `
                <span class="heatmap-clause">${f.clause}</span>
                <div class="heatmap-bar-outer">
                    <div class="heatmap-bar-inner" style="width:${pct}%;background:${col}"></div>
                </div>
                <span class="heatmap-score" style="color:${col}">${pct}%</span>
                <span class="heatmap-status" style="color:${col}">${bdge}</span>`;
            heatGrid.appendChild(div);
        });
    }

    // ── Gap Register (Non-compliant + Partial only, sorted by priority) ──
    const gapBody = document.getElementById("gapBody");
    if (gapBody) {
        const gaps = findings.filter(f => f.status !== "Compliant").sort((a,b) => a.score - b.score);
        document.getElementById("gapCount").textContent = gaps.length + " gap(s) identified";
        if (gaps.length === 0) {
            gapBody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:30px;color:var(--muted)">✅ No gaps — all clauses are compliant.</td></tr>`;
        } else {
            gaps.forEach(f => {
                const priority = f.score < 50
                    ? '<span class="priority-high">HIGH</span>'
                    : f.score < 70
                    ? '<span class="priority-med">MEDIUM</span>'
                    : '<span class="priority-low">LOW</span>';
                const badge = f.status === "Partially Compliant"
                    ? `<span class="badge-p">Partial</span>`
                    : `<span class="badge-nc">Non-Compliant</span>`;
                const tr = document.createElement("tr");
                tr.innerHTML = `
                    <td><span class="clause-chip">${f.clause}</span><div style="font-size:0.77rem;color:var(--muted);margin-top:5px">${f.clause_title}</div></td>
                    <td style="max-width:180px">${f.requirement ? f.requirement.slice(0,100)+"…" : "—"}</td>
                    <td>${badge}<div style="font-size:0.75rem;color:var(--muted);margin-top:4px">${f.score}% readiness</div></td>
                    <td>${priority}</td>
                    <td style="max-width:200px;color:#fbbf24">${f.gap || "—"}</td>
                    <td style="max-width:220px;color:#93c5fd">${f.recommendation || "—"}</td>`;
                gapBody.appendChild(tr);
            });
        }
    }

    // ── Evidence Mapping (all clauses) ──
    const evidenceBody = document.getElementById("evidenceBody");
    if (evidenceBody) {
        findings.forEach(f => {
            const cite   = f.company_citation || {};
            const isoRef = f.iso_reference    || {};
            const badge  = f.status === "Compliant"
                ? `<span class="badge-c">✓ Compliant</span>`
                : f.status === "Partially Compliant"
                ? `<span class="badge-p">⚠ Partial</span>`
                : `<span class="badge-nc">✕ Non-Compliant</span>`;

            const citeHtml = cite.found
                ? `<div class="cite-block">
                       <div class="cite-location">📄 ${cite.document || "Unknown"} &nbsp;·&nbsp; Page <strong>${cite.page_no ?? "?"}</strong>${cite.section_heading ? ' &nbsp;·&nbsp; Section: "' + cite.section_heading + '"' : ""}</div>
                       <div class="cite-excerpt">"${cite.verbatim_excerpt || ""}"</div>
                   </div>`
                : `<div class="no-evidence">⚠ No matching evidence found in the uploaded documents.</div>`;

            const gapHtml = f.gap
                ? `<div class="gap-text">⚠ <strong>Gap:</strong> ${f.gap}</div>` : "";
            const recHtml = f.recommendation
                ? `<div class="rec-text">💡 <strong>Recommendation:</strong> ${f.recommendation}</div>` : "";

            const card = document.createElement("div");
            card.className = "evidence-card";
            card.innerHTML = `
                <div class="ev-head">
                    <span class="clause-chip">${f.clause}</span>
                    <div style="flex:1">
                        <div style="font-size:0.88rem;font-weight:600">${f.clause_title}</div>
                        <span class="iso-ref" style="margin-top:4px;display:inline-flex">🛡 ${isoRef.standard || "ISO"}</span>
                    </div>
                    ${badge}
                    <span style="font-size:0.8rem;color:var(--muted);margin-left:12px">${f.score}%</span>
                </div>
                <div class="ev-body">
                    <div class="summary-text">${f.assessment_summary || ""}</div>
                    ${citeHtml}
                    ${gapHtml}
                    ${recHtml}
                </div>`;
            evidenceBody.appendChild(card);
        });
    }

    // ── Export button ──
    const exportBtn = document.getElementById("exportBtn");
    if (exportBtn) exportBtn.onclick = () => exportReport(data);
}

// ═══════════════════════════════════════════════════════════════════
//  EXPORT — plain-text audit report
// ═══════════════════════════════════════════════════════════════════
function exportReport(data) {
    const ts  = new Date().toLocaleString("en-IN");
    const sep = "=".repeat(72);
    const lin = "-".repeat(72);
    const lines = [
        "ANUPALAN MITRA — ISO COMPLIANCE ASSESSMENT REPORT",
        sep,
        "This report is generated by an AI-powered platform designed to replicate",
        "Big4 audit methodology (Deloitte / PwC / EY / KPMG) for ISO readiness.",
        sep,
        `Framework      : ${data.framework_name}`,
        `Assessment Date: ${ts}`,
        `Overall Score  : ${data.overall_score}%`,
        `Maturity Level : ${data.overall_score >= 80 ? "Advanced" : data.overall_score >= 60 ? "Intermediate" : "Foundational"}`,
        `Clauses Assessed: ${data.total_clauses}`,
        `Compliant      : ${data.compliant}   |   Partial: ${data.partial}   |   Non-Compliant: ${data.non_compliant}`,
        `Time Taken     : ${data.elapsed_seconds || "—"}s`,
        "",
        lin,
        "EXECUTIVE SUMMARY",
        lin,
        `The organisation achieved a compliance maturity score of ${data.overall_score}% against ${data.framework_name}.`,
        `${data.compliant} of ${data.total_clauses} assessed clauses are fully compliant,`,
        `${data.partial} show partial compliance with identified gaps,`,
        `and ${data.non_compliant} are non-compliant and require immediate remediation.`,
        "",
        lin,
        "GAP REGISTER & RECOMMENDED ACTIONS",
        lin,
    ];

    const gaps = (data.findings || []).filter(f => f.status !== "Compliant").sort((a,b) => a.score-b.score);
    if (gaps.length === 0) {
        lines.push("No gaps identified — all clauses are compliant.");
    } else {
        gaps.forEach((f, i) => {
            const priority = f.score < 50 ? "HIGH" : f.score < 70 ? "MEDIUM" : "LOW";
            lines.push(`${i+1}. [${priority}] Clause ${f.clause}: ${f.clause_title}`);
            lines.push(`   Status   : ${f.status} (${f.score}% readiness)`);
            lines.push(`   Gap      : ${f.gap || "—"}`);
            lines.push(`   Action   : ${f.recommendation || "—"}`);
            lines.push("");
        });
    }

    lines.push(lin, "EVIDENCE MAPPING & AUDIT TRAIL", lin);
    (data.findings || []).forEach((f, i) => {
        const cite = f.company_citation || {};
        lines.push(`${i+1}. Clause ${f.clause} — ${f.clause_title}`);
        lines.push(`   Status   : ${f.status}  |  Score: ${f.score}%`);
        lines.push(`   ISO Ref  : ${f.iso_reference?.standard || ""} § ${f.clause}`);
        if (cite.found) {
            lines.push(`   Evidence : ${cite.document}, Page ${cite.page_no}${cite.section_heading ? ', Section "'+cite.section_heading+'"' : ""}`);
            lines.push(`   Excerpt  : "${cite.verbatim_excerpt}"`);
        } else {
            lines.push(`   Evidence : No evidence found in uploaded documents.`);
        }
        lines.push(`   Finding  : ${f.assessment_summary}`);
        lines.push("");
    });

    lines.push(sep);
    lines.push("Generated by Anupalan Mitra — AI-Powered ISO Compliance Intelligence Platform");
    lines.push("Methodology: Gemini 2.5 Flash + ChromaDB ISO Knowledge Base + Big4 Audit Framework");

    const blob = new Blob([lines.join("\n")], { type: "text/plain;charset=utf-8" });
    const a = Object.assign(document.createElement("a"), {
        href: URL.createObjectURL(blob),
        download: `ISO_Compliance_Report_${data.framework}_${Date.now()}.txt`,
    });
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

// ═══════════════════════════════════════════════════════════════════
//  ROUTER
// ═══════════════════════════════════════════════════════════════════
document.addEventListener("DOMContentLoaded", function() {
    if (document.getElementById("dropZone"))     initUploadPage();
    if (document.getElementById("dashLoading"))  initDashboard();
});
