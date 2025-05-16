# XPath Cheat Sheet for Web Scraping with Python (`lxml`, `Selenium`, etc.)

---

## 🌿 XPath Syntax Basics

### 🔹 General Path Selectors

| Syntax         | Description                                | Example Match                          |
|----------------|-------------------------------------------|--------------------------------------|
| `//tag`        | Select all matching elements anywhere     | `//div`, `//h1`, `//a`                |
| `/tag`         | Selects elements at exact location        | `/html/body/h1`                       |
| `.`            | Current node                              | Useful for relative paths             |
| `..`           | Parent of current node                    |                                      |
| `@attr`        | Attribute selector                        | `@href`, `@class`                     |

---

## 🌳 Direct Child vs. Descendant

### 🔸 `A / B` — Direct Child
- Matches only if `B` is an **immediate child** of `A`.

```xpath
//div/span
```
✔️ Matches: `<div><span></span></div>`  
❌ Doesn't match: `<div><p><span></span></p></div>`

---

### 🔸 `A // B` — Descendant (Any Depth)
- Matches `B` that is a **child, grandchild, or deeper** inside `A`.

```xpath
//div//span
```
✔️ Matches **all** spans inside a `<div>`, regardless of depth.

---

### 🔍 Example:
HTML:
```html
<div>
  <span>Direct</span>
  <p><span>Nested</span></p>
</div>
```

| XPath         | Matches             |
|---------------|---------------------|
| `//div/span`  | Only "Direct"       |
| `//div//span` | Both spans          |

---

## 🏷️ Matching Elements by Attribute

| Goal                        | XPath Example                                      |
|-----------------------------|----------------------------------------------------|
| Exact match                 | `//div[@class="card"]`                             |
| Partial match with `contains()` | `//div[contains(@class, "card")]`               |
| Starts with                | `//img[starts-with(@src, "/images/")]`             |
| Multiple attributes        | `//div[@class="card" and @data-role="feature"]`    |

---

## 🔡 Text Matching

| Match Type            | XPath Example                                         | Notes                                |
|-----------------------|------------------------------------------------------|--------------------------------------|
| Exact text            | `//h2[text()="Top Songs"]`                            | Must match exactly                   |
| Contains text         | `//p[contains(text(), "Billboard")]`                 | Partial match                        |
| Trim whitespace       | `//*[normalize-space(text())="Hot 100"]`             | Removes leading/trailing spaces      |
| Extract text          | `//h3/text()`                                        | Gets the actual string value         |

---

## 🔢 Positional Matching

| XPath                                 | Meaning                                  |
|---------------------------------------|------------------------------------------|
| `(//h3)[1]`                           | First matching `<h3>`                    |
| `(//li[@class="track"])[last()]`      | Last matching element                    |
| `(//div[@class="card"])[3]`           | Third match                              |

---

## 🌲 Relative Paths (Scoped Selection)

| XPath Example                                     | Description                                     |
|--------------------------------------------------|-------------------------------------------------|
| `.//span`                                         | All `span` inside the current element           |
| `./span`                                          | Only direct child `span`s of current element    |
| `.//div[@class="meta"]/a`                         | Nested `a` tags inside a scoped `div`           |
| `//ul[@id="items"]//li`                           | Any `li` under a specific `ul`, at any depth    |

---

## 🎧 Billboard Example XPaths

| Data                 | XPath                                                  |
|----------------------|--------------------------------------------------------|
| Chart item container | `//li[contains(@class, "o-chart-results-list__item") and .//h3]` |
| Song title           | `.//h3[contains(@id, "title-of-a-story")]/text()`     |
| Artist name          | `.//span[contains(@class, "c-label") and not(contains(@class, "a-no-trucate"))]/text()` |
| Chart position       | `.//span[contains(@class, "a-font-primary-bold")]/text()` |
| Weeks on chart       | `.//li/span[contains(text(), "Weeks")]/following-sibling::span[1]/text()` |

---

## 🛠 Python Scraping Helpers

```python
from lxml import html
import requests

def clean_text_list(raw_list):
    return [t.strip() for t in raw_list if t.strip()]
```

```python
# Example usage
tree = html.fromstring(response.content)
entries = tree.xpath('//li[contains(@class, "o-chart-results-list__item") and .//h3]')
for entry in entries:
    title = clean_text_list(entry.xpath('.//h3/text()'))
    artist = clean_text_list(entry.xpath('.//span[contains(@class, "c-label") and not(contains(@class, "a-no-trucate"))]/text()'))
```

---

## 🧪 Test Your XPath

- ✅ Chrome DevTools Console:  
  Type `$x('//your/xpath')`
- ✅ Online tools: [https://xpather.com](https://xpather.com)
- ✅ Use `.xpath()` in Python for real content

---
