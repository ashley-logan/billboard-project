# 🐍 Python `re` Module – Regular Expressions Cheat Sheet

---

## 🔰 Basic Setup

```python
import re
```

---

## 🎯 Common Functions

| Function         | Description                                 | Example |
|------------------|---------------------------------------------|---------|
| `re.search()`    | Search for first match                      | `re.search(r"\d+", "Item 42")` |
| `re.match()`     | Match from start of string                  | `re.match(r"Hello", "Hello World")` |
| `re.findall()`   | Return all non-overlapping matches          | `re.findall(r"\w+", "hi there!")` |
| `re.sub()`       | Replace pattern with string                 | `re.sub(r"cat", "dog", "my cat")` |
| `re.split()`     | Split string using pattern as delimiter     | `re.split(r"\s+", "a b  c")` |
| `re.compile()`   | Compile regex pattern for reuse             | `pattern = re.compile(r"\d+")` |

---

## 🧱 Character Classes

| Pattern | Description                          | Example Match        |
|---------|--------------------------------------|----------------------|
| `.`     | Any character except newline         | `a.c` → `abc`, `a1c` |
| `\d`   | Digit (0-9)                          | `\d+` → `42`        |
| `\D`   | Non-digit                            | `\D+` → `abc`       |
| `\w`   | Word character (letters, digits, _)  | `\w+` → `word_123`  |
| `\W`   | Non-word character                   | `\W+` → `!?@`       |
| `\s`   | Whitespace (space, tab, newline)     | `\s+` → spaces      |
| `\S`   | Non-whitespace                       | `\S+` → `abc123`    |

---

## 🔁 Quantifiers

| Pattern    | Description                      | Example Match         |
|------------|----------------------------------|------------------------|
| `*`        | 0 or more                        | `lo*` → `l`, `loo`     |
| `+`        | 1 or more                        | `lo+` → `lo`, `loo`    |
| `?`        | 0 or 1                           | `lo?` → `l`, `lo`      |
| `{n}`      | Exactly n                        | `a{3}` → `aaa`         |
| `{n,}`     | n or more                        | `a{2,}` → `aa`, `aaaa` |
| `{n,m}`    | Between n and m                  | `a{2,4}` → `aa`, `aaa` |

---

## 🧠 Anchors

| Pattern | Description             | Example Match             |
|--------|-------------------------|----------------------------|
| `^`     | Start of string         | `^Hello` → `"Hello world"` |
| `$`     | End of string           | `end$` → `"The end"`       |
| `\b`   | Word boundary           | `\bcat\b` → `cat` only    |
| `\B`   | Non-word boundary       | `\Bend\B`                |

---

## 🎭 Groups & Alternation

| Pattern     | Description                      | Example Match            |
|-------------|----------------------------------|---------------------------|
| `(abc)`     | Group                            | `(ha)+` → `hahaha`        |
| `|`         | OR (alternation)                 | `cat|dog` → `cat` or `dog`|
| `(?:...)`   | Non-capturing group              | `(?:ha)+` → `hahaha`      |
| `(?P<name>...)` | Named group                 | `(?P<word>\w+)`          |

---

## 🔍 Lookahead/Lookbehind

| Pattern         | Description                      | Example Use                |
|------------------|----------------------------------|----------------------------|
| `(?=...)`        | Positive lookahead               | `\d(?=px)` → `42` in `42px`|
| `(?!...)`        | Negative lookahead               | `\d(?!px)` → `42` not `42px`|
| `(?<=...)`       | Positive lookbehind              | `(?<=USD)\d+` → `USD100`   |
| `(?<!...)`       | Negative lookbehind              | `(?<!USD)\d+`              |

---

## ⚙️ Flags

| Flag        | Description                          |
|-------------|--------------------------------------|
| `re.I`      | Ignore case                          |
| `re.M`      | Multiline mode (`^` and `$` on lines)|
| `re.S`      | Dot matches newline                  |
| `re.X`      | Verbose mode (ignore whitespace)     |

---

## 📦 Example Usage

```python
text = "User: Alice, Age: 32"
match = re.search(r"User: (\w+), Age: (\d+)", text)
if match:
    print(match.group(1))  # Alice
    print(match.group(2))  # 32
```

---
