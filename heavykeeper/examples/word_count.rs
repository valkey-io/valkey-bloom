use clap::Parser;
use heavykeeper::TopK;
use memmap2::Mmap;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead};
use std::process::exit;

const MAX_WORD_LEN: usize = 64;

#[derive(Debug, Clone)]
struct Word {
    bytes: [u8; MAX_WORD_LEN],
    len: u8,
}

impl Word {
    fn new() -> Self {
        Word {
            bytes: [0; MAX_WORD_LEN],
            len: 0,
        }
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn push(&mut self, byte: u8) {
        if self.len < MAX_WORD_LEN as u8 {
            self.bytes[self.len as usize] = byte;
            self.len += 1;
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

// Only hash the actual content
impl Hash for Word {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

// Compare only the actual content
impl PartialEq for Word {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Word {}

// Order by actual content
impl PartialOrd for Word {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Word {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl std::fmt::Display for Word {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // we always have valid UTF-8
        let s = unsafe { std::str::from_utf8_unchecked(self.as_slice()) };
        write!(f, "{}", s)
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'k')]
    k: usize,

    #[arg(short = 'w', default_value_t = 8)]
    width: usize,

    #[arg(short = 'd', default_value_t = 2048)]
    depth: usize,

    #[arg(short = 'y', default_value_t = 0.9)]
    decay: f64,

    #[arg(short = 'f')]
    input: Option<String>,
}

fn main() {
    let args = Args::parse();

    let mut topk = TopK::<Word>::new(args.k, args.width, args.depth, args.decay);
    let mut word = Word::new();

    if args.input.is_none() {
        let stdin = io::stdin();
        let mut stdin_lock = stdin.lock();
        let mut buffer = Vec::with_capacity(1024 * 1024);

        while stdin_lock.read_until(b'\n', &mut buffer).unwrap() > 0 {
            process_bytes(&buffer, &mut topk, &mut word);
            buffer.clear();
        }
    } else {
        let file = std::fs::File::open(args.input.unwrap()).unwrap_or_else(|e| {
            eprintln!("Error: {}", e);
            exit(1);
        });

        let mmap = unsafe { Mmap::map(&file) }.unwrap_or_else(|e| {
            eprintln!("Error mapping file: {}", e);
            exit(1);
        });

        process_bytes(&mmap, &mut topk, &mut word);
    }

    for node in topk.list() {
        println!("{} {}", node.item, node.count);
    }
}

fn process_bytes(bytes: &[u8], topk: &mut TopK<Word>, word: &mut Word) {
    let mut pos = 0;
    let len = bytes.len();

    while pos < len {
        // Skip non-alphabetic characters
        while pos < len && !bytes[pos].is_ascii_alphabetic() {
            pos += 1;
        }

        if pos >= len {
            break;
        }

        // Find end of word
        let word_start = pos;
        while pos < len && bytes[pos].is_ascii_alphabetic() {
            pos += 1;
        }

        let word_len = pos - word_start;
        if word_len > 0 && word_len <= MAX_WORD_LEN {
            // Clear and reuse the word
            word.clear();

            // Convert to lowercase while copying
            for &b in &bytes[word_start..pos] {
                word.push(b.to_ascii_lowercase());
            }

            // Add to TopK
            topk.add(word, 1);
        }
    }
}
