from collections import UserDict, defaultdict

class JoiningDict(UserDict):
    
    def outer_join(self, other):
        if not isinstance(other, dict):
            return NotImplemented

        self_ = self.data.copy()
        for key in other:
            self.join(other, key)
        return JoiningDict(self)


    def inner_join(self, other):
        if not isinstance(other, dict):
            return NotImplemented

        self_ = self.data.copy()
        del_keys = self_.keys() - other.keys()

        for key in del_keys:
            del self_[key] 

        for key in self_:
            self.join(other, key)

        return JoiningDict(self)


    def left_join(self, other):
        if not isinstance(other, dict):
            return NotImplemented

        self_ = self.data.copy()
        del_keys = other.keys() - self_.keys()
        for key in del_keys:
            del other[key]

        for key in other:
            self.join(other, key)
            
        return JoiningDict(self)

    def join(self, other, key):
        self_ = defaultdict(list, self.data)

        if key in self_ and not isinstance(self_[key], list):
            self_[key] = [self_[key]]
        if not isinstance(other.get(key), list):
            self_[key].append(other[key])
        else:
            self_[key].extend(other[key])

        self.data = dict(self_)
        




        
        