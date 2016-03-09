class MissingNum(object):
	"""docstring for MissingNum"""
	def __init__(self,arg=None):
		#super(MissingNum, self).__init__()
		self.arg = arg

	#using the sum of all numbers and substract all elemente in the array will return the missing value
	def getMissingValue(self,arr,N):
		if len(arr) == 0:
			raise ValueError("Array %s must have value"%arr)
		elif len(arr) != N:
			raise ValueError("Array %s size is not Same with N %s"%(arr,N))
		else:
			total = (1+N+1)*(N+1)//2 # get sum of all numbers 1..N+1

			for v in arr:
				total -=v

		return total


class ReverseFib(object):
	def __init__(self,arg=None):
		self.arg = arg

	def createRevFib(self, a,b):
		if a<=b or a<0 or b < 0:
			raise ValueError("Invalid value a %s or b %s "%(a,b))

		rfib = [a,b]
		c = 0
		while c >=0:
			c = a-b
			rfib.append(c)
			a,b = b,c
			if b == 0:
				break

		return rfib



def main():
	a =[]
	N= 3
	Missing = MissingNum()
	print Missing.getMissingValue(a,N)

def testFib():
	a,b = 100,50
	RFB =ReverseFib()
	result = RFB.createRevFib(a,b)

	print result



if __name__ == '__main__':
	#main()
	testFib()


def r(a,b):
     r = [a,b]
     c = 0
     while c>=0:
             c = a-b
             r.append(c)
             a,b = b,c
             if b ==0:
             	break
     return r


 def merge2(A, m, B, n):
        lastA, lastB = m-1, n-1
        last2Write = m + n -1
        while lastA != -1 and lastB != -1:
            if A[lastA] >= B[lastB]:
                A[last2Write] = A[lastA]
                lastA -= 1
            else:
                A[last2Write] = B[lastB]
                lastB -= 1
            last2Write -= 1

        # If A list has some remaining items to process, they are already in
        # their to-be position. No operation is needed.
        # Otherwise, if B list has some remaining items, copy them to the head
        # of A.
        if lastB != -1:  A[:lastB+1] = B[:lastB+1]

        return

def merge2(l, m):
	if len(l) ==0 or len(m) == 0:
		raise ValueError("Invalid arrays, can't be null ")
    result = []
    i = j = 0
    total = len(l) + len(m)
    while len(result) != total:
        if len(l) == i:
            result += m[j:]
            break
        elif len(m) == j:
            result += l[i:]
            break
        elif l[i] < m[j]:
            result.append(l[i])
            i += 1
        else:
            result.append(m[j])
            j += 1
    return result

def merge3Arrays(a,b,c):
	temp = merge2(a,b)
	return merge2(temp,c)


class Solution(object):
    def maxProduct(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        #need to store min, and max
        ma,mi,max_so_far = nums[0],nums[0],nums[0]
        for v in nums[1:]:
            mx = ma
            mn = mi
            ma = max(max(mx*v,v),mn*v)
            mi = min(min(mn*v,v),mx*v)
            max_so_far = max(ma,max_so_far)
        return max_so_far

'''

Version B:

push:
enqueue in queue2
enqueue all items of queue1 in queue2, then switch the names of queue1 and queue2
pop:
deqeue from queue1
'''




class Solution(object):
    def mergeTwoLists(self, l1, l2):
        """
        :type l1: ListNode
        :type l2: ListNode
        :rtype: ListNode
        """
        temp = ListNode(0)
        p = temp
        while l1 and l2:
            if l1.val <l2.val:
                p.next = l1
                l1 = l1.next
            else:
                p.next = l2
                l2 = l2.next
            p = p.next

        if l1:p.next = l1
        if l2:p.next = l2
        return temp.next

class Solution(object):
    def merge(self, nums1, m, nums2, n):
        """
        :type nums1: List[int]
        :type m: int
        :type nums2: List[int]
        :type n: int
        :rtype: void Do not return anything, modify nums1 in-place instead.
        """
        #assert len(nums1) < m+n
        #len = m+n-1
        i,j,k= m-1,n-1,m+n-1
        while i>=0 and j>=0 :
            if nums1[i] > nums2[j]:
                nums1[k] = nums1[i]
                i-=1
            else:
                nums1[k] = nums2[j]
                j-= 1
            k-=1

        while j>=0: # array A used up, array b still has ele
            nums1[k]=nums2[j]
            j-=1
            k-=1
