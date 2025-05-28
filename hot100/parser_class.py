from lxml import HTMLPullParser

class HTMLTargetParser(HTMLPullParser):

    def __init__(self, **kwargs):
        self.config = kwargs
        self.state = "default"
        self.at_quota = False
        super().__init__(events=events, tag=parent_ele["tag"])
        self.STATES = {
            "default": on_default,
            "within_row": in_row
        }
        self.row = []
        self.data = []

    def feed(self, chunk, **kwargs):
        super().feed(chunk, **kwargs)
        for event, ele in self.read_events():
            self.on_event(event, ele)


    def get_data(self):
        assert len(self.data) >= self.config["num_scrapes"]
        if self.row:
            self.data += self.row
        return self.data


    def on_event(self, event, ele):
        # #hands the incoming event and element to on_default()
        # or in_row() depending on whether the parser is currently within
        # the parent element defined in self.config
        self.STATES[self.state](event, ele)


    def on_default(self, event, ele):
        # if not currently within the parent element, checks
        # checks if the parser has now entered the parent element
        if event == "start" and self.is_parent(ele, **self.config):
            curr_row = []
            self.state = "within_row"



    def in_row(self, event, ele):
        # if within the parent element, checks if the parser has hit the parent's
        # closing tag or has entered a target element and if so loads or extracts
        # the row data
        num_scrapes = self.config["num_scrapes"]
        parser_ignore = self.config["parser_ignore"]
        if event == "end" and self.is_parent(ele, self.config["parent_ele"]):
            self.data += self.row
            self.row = []
            self.state = "default"
            if len(self.data) >= num_scrapes:
                self.at_quota = True
        elif event == "end" and self.is_target(ele, self.config["target_eles"]):
            text = ele.text.strip()
            if text and text not in parser_ignore:
                self.row += text
            text = ""

    def is_parent(self, ele, parent_ele):
        # return if element is parent element
        return ele.tag == parent_ele["tag"] and parent_ele["class_"] in ele.attrib.get("class")

    def is_target(self, ele, target_eles):
        # returns if element is one of the target elements
        for tag in target_eles["tags"]:
            if ele.tag == tag:
                return True
        return False

