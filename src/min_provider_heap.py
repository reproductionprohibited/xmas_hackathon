from typing import Dict, List
from provider import Provider
from copy import deepcopy


class MinProviderHeap:
    def __init__(self):
        self.heap: List[Provider] = []
        self.conditions: Dict[int, int] = {}
        
    def push(self, x: Provider):
        x_copied = deepcopy(x)
        self.conditions[x_copied.id] = x_copied.version
        self.heap.append(x_copied)

        v = len(self.heap) - 1
        while (v > 0 and self.heap[v] < self.heap[(v - 1) >> 1]):
            self.heap[v], self.heap[(v - 1) >> 1] = self.heap[(v - 1) >> 1], self.heap[v]
            v = (v - 1) >> 1

    def pop(self):
        if (len(self.heap) == 0):
            return None
        ans = self.top()

        # dbg("::")
        # self.print()

        if (self.size() == 0):
            return None
        self.heap[0], self.heap[len(self.heap) - 1] = self.heap[len(self.heap) - 1], self.heap[0]
        self.heap.pop()
        v = 0
        while (v * 2 + 1 < len(self.heap)):
            if (self.heap[v * 2 + 1] < self.heap[v] and (v * 2 + 2 == len(self.heap) or self.heap[v * 2 + 2] < self.heap[v * 2 + 1])):
                self.heap[v], self.heap[v * 2 + 1] = self.heap[v * 2 + 1], self.heap[v]
                v = v * 2 + 1
            elif (v * 2 + 2 < len(self.heap) and self.heap[v * 2 + 2] < self.heap[v]):
                self.heap[v], self.heap[v * 2 + 2] = self.heap[v * 2 + 2], self.heap[v]
                v = v * 2 + 2
            else:
                break
        
        return ans
    
    def pop_no_return(self):
        if (len(self.heap) == 0):
            return None
        self.heap[0], self.heap[len(self.heap) - 1] = self.heap[len(self.heap) - 1], self.heap[0]
        self.heap.pop()
        v = 0
        while (v * 2 + 1 < len(self.heap)):
            if (self.heap[v * 2 + 1] < self.heap[v] and (v * 2 + 2 == len(self.heap) or self.heap[v * 2 + 2] < self.heap[v * 2 + 1])):
                self.heap[v], self.heap[v * 2 + 1] = self.heap[v * 2 + 1], self.heap[v]
                v = v * 2 + 1
            elif (v * 2 + 2 < len(self.heap) and self.heap[v * 2 + 2] < self.heap[v]):
                self.heap[v], self.heap[v * 2 + 2] = self.heap[v * 2 + 2], self.heap[v]
                v = v * 2 + 2
            else:
                break

    def size(self) -> int:
        return len(self.heap)

    def top(self):
        while (self.size() > 0 and self.heap[0].version != self.conditions[self.heap[0].id]):
            # dbg("HUY")
            # dbg(f"{self.heap[0].id} {self.heap[0].version} {self.conditions[self.heap[0].id]}")
            self.pop_no_return()
        if (self.size() == 0):
            return None
        else:
            return self.heap[0]

    # def print(self):
    #     dbg(self.conditions)
    #     for i in self.heap:
    #         dbg(f": {i.id} {i.version}")
    #     dbg("")
