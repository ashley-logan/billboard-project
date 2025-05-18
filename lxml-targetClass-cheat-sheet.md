# 🧩 lxml Target Class Cheat Sheet

### ✅ What is a Target Class?
A **target class** defines **what happens as the parser encounters XML elements**. It's a way to **handle events** like tag starts, text content, and ends — **without building a full tree in memory** unless you choose to.

---

## 🔧 Target Interface: Required Methods

| Method           | Called When...                          | Returns / Purpose                          |
|------------------|------------------------------------------|---------------------------------------------|
| `start(tag, attrib)` | Opening tag is encountered            | You can capture tag/attributes              |
| `end(tag)`       | Closing tag is encountered               | Return value becomes child of parent (optional) |
| `data(text)`     | Text between tags is encountered         | Capture character data                      |
| `close()`        | Parsing is complete                      | Return the final result (e.g. a list or root) |

---

## 📦 Minimal Working Example

```python
from lxml import etree

class MyTarget:
    def __init__(self):
        self.data = []

    def start(self, tag, attrib):
        pass  # handle tag start

    def end(self, tag):
        pass  # handle tag end

    def data(self, text):
        self.data.append(text.strip())

    def close(self):
        return self.data
```

### 🧪 Use It:

```python
parser = etree.XMLParser(target=MyTarget())
result = etree.fromstring("<root>Hello<child>world</child></root>", parser)
print(result)  # ['Hello', 'world']
```

---

## 🧱 Full Example: Extract `<item>` Tags Only

```python
class ItemExtractor:
    def __init__(self):
        self.stack = []
        self.items = []
        self.capture = False

    def start(self, tag, attrib):
        if tag == "item":
            self.capture = True
            el = etree.Element(tag, attrib)
            self.stack.append(el)
        elif self.capture:
            el = etree.Element(tag, attrib)
            self.stack[-1].append(el)
            self.stack.append(el)

    def end(self, tag):
        if self.capture:
            el = self.stack.pop()
            if not self.stack:  # finished <item>
                self.items.append(el)
                self.capture = False

    def data(self, text):
        if self.capture and self.stack:
            self.stack[-1].text = (self.stack[-1].text or "") + text

    def close(self):
        return self.items
```

---

## 🧠 Tips

| Goal                          | Tip                                       |
|------------------------------|-------------------------------------------|
| Ignore most tags             | Only react in `start()` to relevant tags  |
| Reduce memory                | Don’t build full tree unless needed       |
| Add helpers                  | Add custom methods — totally allowed      |
| Capture specific tags only   | Use `self.capture` flag logic             |
| Combine with streaming       | Use with `parser.feed(chunk)` for large files |

---

## 🧪 Example Usage with Feed

```python
target = ItemExtractor()
parser = etree.XMLParser(target=target)

with open("big.xml", "rb") as f:
    for chunk in f:
        parser.feed(chunk)

items = parser.close()
```

---

## ✅ Summary

| What You Do              | How                            |
|--------------------------|---------------------------------|
| React to tag start       | `start(tag, attrib)`            |
| React to tag end         | `end(tag)`                      |
| Collect text             | `data(text)`                    |
| Return final result      | `close()`                       |
| Optional logic helpers   | Add your own methods/attributes |
