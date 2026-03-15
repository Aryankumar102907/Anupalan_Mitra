import sys
sys.path.append('.')
from chromadb import PersistentClient
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

CHROMA_DIR = "chroma_db"
COLLECTION_NAME = "iso_frameworks"

def test_top_k(query: str, iso_name: str, max_k: int = 15):
    print(f"\nEvaluating Top-K for '{iso_name}'")
    print(f"Query: {query}\n" + "-"*60)
    
    client = PersistentClient(path=CHROMA_DIR)
    ef = DefaultEmbeddingFunction()
    
    try:
        col = client.get_collection(COLLECTION_NAME, embedding_function=ef)
    except Exception as e:
        print(f"Error loading collection: {e}")
        return
        
    results = col.query(
        query_texts=[query],
        n_results=max_k,
        where={"source": iso_name},
        include=["documents", "metadatas", "distances"]
    )
    
    docs = results["documents"][0]
    distances = results["distances"][0]
    
    total_chars = 0
    
    print(f"{'K':<5} | {'Dist(L2)':<10} | {'Chars added':<12} | {'Cumul Chars':<12} | Relevance")
    print("-" * 60)
    
    for i, (doc, dist) in enumerate(zip(docs, distances), 1):
        chars = len(doc)
        total_chars += chars
        
        # Lower distance = closer/more relevant
        if dist < 0.6: rel = "⭐⭐⭐ High"
        elif dist < 0.8: rel = "⭐⭐ Medium"
        elif dist < 1.0: rel = "⭐ Low"
        else: rel = "❌ Irrelevant Noise"
            
        print(f"{i:<5} | {dist:<10.3f} | {chars:<12} | {total_chars:<12} | {rel}")

if __name__ == "__main__":
    test_top_k(
        query="The organization has identified and documented the internal and external issues relevant to its purpose that affect the ABMS, including bribery risk exposure from its sector, geography, and business model.",
        iso_name="ISO 37001 (Anti-bribery Management Systems)"
    )
