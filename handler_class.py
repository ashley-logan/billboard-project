


class HTMLHandler:
    PARENT_TAGS = []  # parent tag that contains all relevant nodes
    CHILD_TAGS = []  # tags within parent node that contain relevant data
    PARENT_CLASSES = []  # beginning of the parent class
    TEXT_IGNORE = []  # irrelevant text that shouldn't be returned

    def __init__(self):
        self.content = []  # returns all of the rows as lists of strings
        self.curr_row = []  # holds the data as a list of strings
        self.state = "default"

        self.STATES = {"default": self.handle_default, "in_row": self.handle_in_row}

    def is_parent(self, ele):
        for TAG, CLASS_ in zip(self.PARENT_TAGS, self.PARENT_CLASSES):
            if ele.tag == TAG and CLASS_ in ele.attrib.get("class", ""):
                return True
        return False


    def is_target(self, ele):
        return ele.tag in self.CHILD_TAGS

    def handle_default(self, event, ele):
        if event == "start" and self.is_parent(ele):
            self.state = "in_row"

    def handle_in_row(self, event, ele):
        if event == "end" and self.is_parent(ele):
            self.state = "default"
            assert self.curr_row
            self.content.append(self.curr_row)
            self.curr_row = []
        elif event == "end" and self.is_target(ele):
            data = self.clean_text(ele)
            if data:
                self.curr_row.append(data)

    def on_event(self, event, ele):
        self.STATES[self.state](event, ele)
        if event == "end":
            ele.clear()

    def __call__(self, event, ele):
        self.on_event(event, ele)

    def clean_text(self, ele):
        if not ele.text or ele.text.strip() in self.TEXT_IGNORE:
            return None
        else:
            return ele.text.strip()

    def get_content(self):
        if self.curr_row:
            self.content.append(self.curr_row)
        result = self.content
        self.content = []
        return result

    def __len__(self):
        return len(self.content)

    def __str__(self):
        output = ""
        for i, row in enumerate(self.content):
            output += f"row {i}: {row}\n"
        return output