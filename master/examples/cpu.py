from math import sqrt
for i in range(50000):
	eh = True
	for j in range(2, i):		
		if (i%j)==0:
			eh = False
			break
	if(eh): print(i, end=',')