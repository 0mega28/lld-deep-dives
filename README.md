# lld-deep-dives

Low-level design, machine coding, and concurrency deep dives. Each folder is a small,
self-contained implementation that backs a post on [my blog](https://0mega28.github.io/blog/).

## Deep dives

| Project | Topic | Blog post |
| --- | --- | --- |
| [mvcc-kv-store](mvcc-kv-store/) | Lock-free reads, atomic multi-key batches, and ordered commits in an in-process MVCC key-value store (Java) | [Three places my MVCC design broke when I wrote the code](https://0mega28.github.io/blog/mvcc-design-breaks) |

## Layout

```
lld-deep-dives/
├── <project>/
│   ├── README.md   # what it does, design notes, how to run
│   └── ...         # source
├── LICENSE
└── README.md       # this index
```

Each project has its own README with run instructions and toolchain requirements.

## License

MIT
