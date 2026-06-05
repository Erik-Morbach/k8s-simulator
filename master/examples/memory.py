import time
import random
testMemory = [random.random() for _ in range(100000000)]

for i in range(100):
	for j in range(len(testMemory)):
		testMemory[j] += random.random()
	time.sleep(0.1)