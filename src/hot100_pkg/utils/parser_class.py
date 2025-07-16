from lxml import etree


class HTMLTargetParser(etree.HTMLPullParser):
    def __init__(self, config: dict):
        self.config = config
        self.state = "default"
        self.at_quota = False
        super().__init__(events=self.config["events"])
        self.row = []
        self.data = []
        self.STATES = {"default": self.on_default, "within_row": self.in_row}

    def feed(self, chunk, **kwargs):
        super().feed(chunk, **kwargs)
        for event, ele in self.read_events():
            # print(event, ele)
            self.on_event(event, ele)

    def get_data(self) -> list[list[str]]:
        # assert len(self.data) >= self.config["num_scrapes"]
        if self.row:
            self.data.append(self.row)
        return self.data

    def on_event(self, event: str, ele: etree.Element):
        # #hands the incoming event and element to on_default()
        # or in_row() depending on whether the parser is currently within
        # the parent element defined in self.config
        self.STATES[self.state](event, ele)

    def on_default(self, event: str, ele: etree.Element):
        # if not currently within the parent element, checks
        # checks if the parser has now entered the parent element
        if event == "start" and self.is_parent(ele, self.config["parent_ele"]):
            self.row = []
            self.state = "within_row"

    def in_row(self, event: str, ele: etree.Element):
        # if within the parent element, checks if the parser has hit the parent's
        # closing tag or has entered a target element and if so loads or extracts
        # the row data
        if event == "end" and self.is_parent(ele, self.config["parent_ele"]):
            self.data.append(self.row)
            self.row = []
            self.state = "default"
            if len(self.data) >= self.config["num_scrapes"]:
                self.at_quota = True
        elif event == "end" and self.is_target(ele, self.config["target_eles"]):
            text = ele.text.strip()
            if text and text not in self.config["parser_ignore"]:
                self.row.append(text)
            text = ""

    def is_parent(self, ele: etree.Element, parent_ele: dict):
        # return if element is parent element
        return ele.tag == parent_ele["tag"] and parent_ele["class_"] in ele.attrib.get(
            "class"
        )

    def is_target(self, ele: etree.Element, target_eles: dict):
        # returns if element is one of the target elements
        for tag in target_eles["tags"]:
            if ele.tag == tag:
                return True
        return False
