def fib(n):
	a=0;b=1
	for i in range(n):
		a,b=b,a+b
	return a

class Solution(object):
    def maxProfit(self, prices):
        """
        :type prices: List[int]
        :rtype: int
        """
        maxDiff,sellIndex,current_min= 0,0,prices[0] if prices else 0
        for i in prices[1:]:
            current_min = min(current_min,i)
            if maxDiff < i-current_min:
                maxDiff = i-current_min
        return maxDiff


def invertTree(self, root):
        '''
        if root:
            tmp = root.left
            root.left = self.invertTree(root.right)
            root.right = self.invertTree(tmp)
        return root
        '''
        if root:
            root.left,root.right = self.invertTree(root.right),self.invertTree(root.left)
        return root

# two stacks make a queue
class Queue:
    # initialize your data structure here.
    def __init__(self):
        self.s1 = []
        self.s2 = []


    # @param x, an integer
    # @return nothing
    def push(self, x):
        self.s1.append(x)


    # @return nothing
    def pop(self):
        if not self.s2:
            while self.s1:
                self.s2.append(self.s1.pop())

        self.s2.pop()
        return


    # @return an integer
    def peek(self):
        if self.s2:
            return self.s2[-1]
        while self.s1:
            self.s2.append(self.s1.pop())
        return self.s2[-1]


    # @return an boolean
    def empty(self):
        return True if not self.s1 and not self.s2 else False


class Stack(object):
    def __init__(self):
        """
        initialize your data structure here.
        """
        self.q1=[]
        self.q2=[]


    def push(self, x):
        """
        :type x: int
        :rtype: nothing
        """
        self.q2.append(x) # make push costly , enque q2 for new ele, and deq all from q1 to q2
        while self.q1:
            self.q2.append(self.q1.pop(0))
        self.q1,self.q2 = self.q2,self.q1

    def pop(self):
        """
        :rtype: nothing
        """
        self.q1.pop(0) # q1 always pop the newest element



    def top(self):
        """
        :rtype: int
        """
        return self.q1[0]

    def empty(self):
        """
        :rtype: bool
        """
        return True if not self.q1 and not self.q2 else False


#
class Solution:
    # @param {integer} n
    # @return {boolean}
    def isPowerOfTwo(self, n,base=None):
        if n in [1,2]:
            return True
        temp =base if base else 2
        while(temp <=n):
            if temp == n:
                return True
            temp = temp*base

        return False

# Definition for a binary tree node.
# class TreeNode:
#     def __init__(self, x):
#         self.val = x
#         self.left = None
#         self.right = None

class Solution:
    # @param {TreeNode} root
    # @param {TreeNode} p
    # @param {TreeNode} q
    # @return {TreeNode} BST
    def lowestCommonAncestor(self, root, p, q):
        if not root or not p or not q:
            return None
        if max(p.val,q.val) <root.val:
            return self.lowestCommonAncestor(root.left,p,q)
        elif min(p.val,q.val) >root.val:
            return self.lowestCommonAncestor(root.right,p,q)
        else:
            return root


#delete a node in linkedlist
#1->2>3->4 ,
class Solution(object):
    def deleteNode(self, node):
        """
        :type node: ListNode
        :rtype: void Do not return anything, modify node in-place instead.
        """
        node.val = node.next.val
        node.next =node.next.next

N = input()
max_num = range(N)
s = raw_input()
AP = map(int,s.split())
comm_dif = AP[1]-AP[0]
length = len(AP)
for i in range(N):
    if i != length-1:
        if AP[i+1]-AP[i] != comm_dif:
            print AP[i]+comm_dif


name,seq= None,[]
for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            name = line.replace('>','').replace(' ',',')
            seq = []
        else:
            seq.append(line)
if name:
        return "{},{}".format(name,','.join(seq))


import sys


base = {
    0: '', 1: 'One', 2: 'Two', 3: 'Three', 4: 'Four', 5: 'Five', 6: 'Six',
    7: 'Seven', 8: 'Eight', 9: 'Nine', 10: 'Ten', 11: 'Eleven', 12: 'Twelve',
    13: 'Thirteen', 14: 'Fourteen', 15: 'Fifteen', 16: 'Sixteen',
    17: 'Seventeen', 18: 'Eighteen', 19: 'Nineteen'
}

tens = {
    2: 'Twenty', 3: 'Thirty', 4: 'Forty', 5: 'Fifty', 6: 'Sixty', 7: 'Seventy',
    8: 'Eighty', 9: 'Ninety'
}

suffix = {0: '', 1: 'Thousand', 2: 'Million', 3: 'Billion', 4: 'Trillion'}

def int_to_text(n):
    def block_to_text(n):
        text = '' if n < 100 else base[n // 100] +'Hundred'

        if n % 100 < 10:
            text += base[n % 10]
        elif 10 <= n % 100 <= 19:
            text += base[n % 100]
        else:
            text += tens[(n % 100) // 10] + base[n % 10]

        return text


    text = 'Dollars'
    depth = 0

    while n > 0:
        block = block_to_text(n % 1000)
        if block:
            text = block + suffix[depth] + text

        n //= 1000
        depth += 1

    return text



if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        for line in f:
            if line.strip():
                print(int_to_text(int(line)))


import re

def mycmp(version1, version2):
    def normalize(v):
        return [int(x) for x in re.sub(r'(\.0+)*$','', v).split(".")]
    return cmp(normalize(version1), normalize(version2))


 def isPalin(s):
    s=re.sub('[^0-9a-zA-Z]','',s.lower())
        l= len(s)
        for i in range(l//2):
           if s[i] !=s[l-i-1]:
             return False
    return True



class Solution(object):
    def firstBadVersion(self, n):
        """
        :type n: int
        :rtype: int
        """
        l,r = 1,n
        while l<r:
            mid = l+(r-l)//2
            if isBadVersion(mid):
                r = mid
            else:
                l = mid +1
        return r




