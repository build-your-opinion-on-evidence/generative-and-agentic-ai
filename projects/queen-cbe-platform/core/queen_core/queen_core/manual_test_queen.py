import argparse
from typing import Any, Dict, List

from queen_core.queen import load_index_from_disk, queen_orchestrator


def print_passages(passages: List[Dict[str, Any]], max_passages: int = 3) -> None:
    """
    Pretty-print top retrieved passages for debugging from CLI.
    """
    print("\n--- Top passages ---")
    if not passages:
        print("No passages returned.")
        return

    for i, p in enumerate(passages[:max_passages], start=1):
        source = p.get("source", "unknown")
        score = float(p.get("score", 0.0))
        text = p.get("text", "")

        print(f"[{i}] Source: {source}  |  score={score:.3f}")
        if len(text) > 600:
            text = text[:600] + " ..."
        print(text)
        print("-" * 60)


def run_single_query(query: str, top_k: int = 5) -> None:
    """
    Run a single query against the Queen orchestrator and print the result.
    """
    print("Loading FAISS index and metadata from disk...")
    index, docs = load_index_from_disk()
    print(f"Index loaded. Number of chunks: {len(docs)}")

    print(f"\n>>> Query: {query}\n")

    result = queen_orchestrator(query=query, index=index, docs=docs, top_k=top_k)
    answer = result["answer"]
    passages = result["passages"]
    log_file_path = result.get("log_file_path")

    print("\n=== Queen answer ===\n")
    print(answer)

    print_passages(passages)

    if log_file_path:
        print(f"\nLog file saved to: {log_file_path}")


def interactive_loop(top_k: int = 5) -> None:
    """
    Interactive query loop for manually testing the Queen orchestrator.
    """
    print("Loading FAISS index and metadata from disk...")
    index, docs = load_index_from_disk()
    print(f"Index loaded. Number of chunks: {len(docs)}")
    print("\nEntering interactive mode. Type 'exit' or 'quit' to stop.\n")

    while True:
        try:
            query = input("Your question > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not query:
            continue
        if query.lower() in ("exit", "quit"):
            print("Exiting.")
            break

        print(f"\n>>> Query: {query}\n")

        result = queen_orchestrator(query=query, index=index, docs=docs, top_k=top_k)
        answer = result["answer"]
        passages = result["passages"]
        log_file_path = result.get("log_file_path")

        print("\n=== Queen answer ===\n")
        print(answer)

        print_passages(passages)

        if log_file_path:
            print(f"\nLog file saved to: {log_file_path}")

        print("\n" + "=" * 80 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manual CLI tester for Queen RAG orchestrator."
    )
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        help="Single query to run once and exit. If omitted, interactive mode is used.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of passages to retrieve per query.",
    )
    args = parser.parse_args()

    if args.query:
        q = args.query.strip()
        if not q:
            print("Empty query string. Nothing to run.")
            return
        run_single_query(query=q, top_k=args.top_k)
    else:
        interactive_loop(top_k=args.top_k)


if __name__ == "__main__":
    main()

