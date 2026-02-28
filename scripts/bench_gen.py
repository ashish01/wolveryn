#!/usr/bin/env python3
"""Generate synthetic JSONL dataset for benchmarking.

Usage:
    python3 bench_gen.py [--docs N] [--output FILE]

Default: 10000 docs to stdout. Set --docs 10000000 for 10M benchmark.
"""

import sys
import json
import random
import string

# Simple LCG random number generator for reproducibility without imports
class LCG:
    def __init__(self, seed=42):
        self.state = seed
    
    def next(self):
        self.state = (self.state * 1103515245 + 12345) & 0x7FFFFFFF
        return self.state
    
    def randint(self, lo, hi):
        return lo + self.next() % (hi - lo + 1)
    
    def choice(self, lst):
        return lst[self.next() % len(lst)]

# ~2000 common English words for generating realistic-ish documents
WORDS = [
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "it",
    "for", "not", "on", "with", "he", "as", "you", "do", "at", "this",
    "but", "his", "by", "from", "they", "we", "say", "her", "she", "or",
    "an", "will", "my", "one", "all", "would", "there", "their", "what",
    "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
    "when", "make", "can", "like", "time", "no", "just", "him", "know",
    "take", "people", "into", "year", "your", "good", "some", "could",
    "them", "see", "other", "than", "then", "now", "look", "only", "come",
    "its", "over", "think", "also", "back", "after", "use", "two", "how",
    "our", "work", "first", "well", "way", "even", "new", "want", "because",
    "any", "these", "give", "day", "most", "us", "great", "between", "need",
    "large", "often", "important", "long", "thing", "right", "old", "big",
    "high", "different", "small", "next", "early", "young", "last", "own",
    "public", "bad", "same", "able", "state", "every", "school", "still",
    "number", "part", "turn", "real", "leave", "might", "develop", "case",
    "world", "start", "hand", "political", "show", "try", "head", "stand",
    "group", "begin", "seem", "country", "help", "point", "end", "change",
    "play", "study", "follow", "since", "move", "include", "believe",
    "allow", "lead", "live", "hold", "write", "provide", "continue",
    "set", "learn", "plant", "cover", "food", "sun", "four", "thought",
    "let", "keep", "eye", "never", "result", "read", "line", "water",
    "city", "run", "late", "interest", "may", "form", "story", "open",
    "grow", "mile", "animal", "light", "home", "walk", "white", "level",
    "test", "model", "power", "software", "computer", "system", "program",
    "question", "build", "stay", "fall", "reach", "rest", "send", "air",
    "data", "network", "service", "create", "search", "engine", "index",
    "query", "document", "term", "score", "rank", "algorithm", "performance",
    "memory", "disk", "cache", "buffer", "thread", "process", "server",
    "client", "request", "response", "database", "table", "record", "field",
    "value", "key", "hash", "tree", "graph", "node", "edge", "path",
    "sort", "merge", "binary", "linear", "constant", "variable", "function",
    "class", "object", "method", "interface", "type", "string", "array",
    "list", "map", "set", "queue", "stack", "heap", "priority", "balance",
    "red", "black", "blue", "green", "yellow", "color", "size", "weight",
    "height", "width", "depth", "length", "area", "volume", "speed",
    "fast", "slow", "quick", "efficient", "optimal", "parallel", "concurrent",
    "distributed", "scalable", "reliable", "secure", "robust", "flexible",
    "simple", "complex", "basic", "advanced", "modern", "classic", "standard",
    "custom", "default", "config", "setting", "option", "parameter", "argument",
    "input", "output", "file", "directory", "folder", "archive", "compress",
    "encrypt", "decode", "encode", "parse", "format", "convert", "transform",
    "filter", "select", "insert", "update", "delete", "remove", "add",
    "push", "pop", "shift", "rotate", "swap", "copy", "clone", "deep",
    "shallow", "reference", "pointer", "address", "offset", "segment",
    "block", "chunk", "batch", "stream", "flow", "pipe", "channel",
    "signal", "event", "handler", "listener", "callback", "promise",
    "future", "async", "await", "sync", "lock", "mutex", "semaphore",
    "condition", "barrier", "atomic", "volatile", "static", "dynamic",
    "compile", "runtime", "debug", "release", "profile", "trace", "log",
    "error", "warning", "info", "verbose", "silent", "quiet", "loud",
    "noise", "clean", "dirty", "fresh", "stale", "hot", "cold", "warm",
    "cool", "temperature", "pressure", "force", "energy", "mass", "charge",
    "wave", "particle", "quantum", "relativity", "gravity", "magnetic",
    "electric", "nuclear", "chemical", "biological", "genetic", "evolution",
    "natural", "artificial", "machine", "learning", "neural", "deep",
    "training", "inference", "prediction", "classification", "regression",
    "clustering", "embedding", "vector", "matrix", "tensor", "dimension",
    "feature", "label", "sample", "batch", "epoch", "iteration", "gradient",
    "optimization", "loss", "accuracy", "precision", "recall", "measure",
    "evaluate", "validate", "train", "test", "split", "cross", "fold",
    "random", "seed", "generate", "produce", "consume", "supply", "demand",
    "market", "price", "cost", "revenue", "profit", "loss", "gain",
    "investment", "return", "risk", "portfolio", "asset", "liability",
    "equity", "debt", "bond", "stock", "share", "dividend", "interest",
    "rate", "inflation", "growth", "recession", "recovery", "expansion",
    "contraction", "cycle", "trend", "pattern", "signal", "noise",
    "analysis", "synthesis", "design", "implement", "deploy", "monitor",
    "maintain", "support", "troubleshoot", "diagnose", "repair", "replace",
    "upgrade", "migrate", "backup", "restore", "recover", "protect",
    "defend", "attack", "threat", "vulnerability", "exploit", "patch",
    "security", "privacy", "identity", "authentication", "authorization",
    "permission", "access", "control", "audit", "compliance", "regulation",
    "policy", "governance", "framework", "methodology", "practice", "pattern",
    "principle", "concept", "theory", "hypothesis", "experiment", "observation",
    "measurement", "calculation", "simulation", "modeling", "abstraction",
    "implementation", "specification", "documentation", "requirement",
    "architecture", "infrastructure", "platform", "environment", "ecosystem",
    "community", "organization", "team", "project", "product", "service",
    "application", "solution", "innovation", "technology", "science",
    "engineering", "mathematics", "physics", "chemistry", "biology",
    "medicine", "health", "education", "research", "development", "testing",
]


def generate_title(rng, word_pool):
    length = rng.randint(3, 8)
    return " ".join(rng.choice(word_pool) for _ in range(length))


def generate_body(rng, word_pool):
    length = rng.randint(30, 150)
    return " ".join(rng.choice(word_pool) for _ in range(length))


def main():
    num_docs = 10000
    output = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] in ("--docs", "-n"):
            i += 1
            num_docs = int(args[i])
        elif args[i] in ("--output", "-o"):
            i += 1
            output = args[i]
        elif args[i] in ("--help", "-h"):
            print(__doc__)
            return
        i += 1

    rng = LCG(42)

    out = open(output, "w") if output else sys.stdout

    for doc_id in range(num_docs):
        doc = {
            "id": doc_id,
            "title": generate_title(rng, WORDS),
            "body": generate_body(rng, WORDS),
        }
        out.write(json.dumps(doc) + "\n")

        if (doc_id + 1) % 1000000 == 0:
            print(f"  Generated {doc_id + 1:,} docs...", file=sys.stderr)

    if output:
        out.close()

    print(f"Generated {num_docs:,} documents", file=sys.stderr)


if __name__ == "__main__":
    main()
