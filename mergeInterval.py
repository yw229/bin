
def team_vacation(list_people_vacations):
	newl = sorted(list_people_vacations,key=lambda tp: tp[0]) #sort in lower bound ASC order
	result = []
	for t in newl:
		if not result:
			result.append(t) #add element into the new list
		else:
			lower = result[-1] #pop the current result, lower one
			# test overlapped between lower and current , lower[0] <= current[0] always
			if lower[1] >= t[0]: #lower' right bound >=current left bound
				up_bound = max(lower[1],t[1]) #pick the max between both right round
				result[-1] =(lower[0],up_bound)
			else: #no intervals, add current into the result
				result.append(t)
	return result

if __name__ == '__main__':

    l = [(4, 7), (5, 10),(1,3)]
    print("Original list : {}".format(l))
    team_vacation = team_vacation(l)
    print("List of ranges after : {}".format(team_vacation))



