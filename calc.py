filenames = ['sub.time', 'event.time']
result = []
for fn in filenames:
  with open(fn) as f:
    try:
      start = int(f.readline())
      end = int(f.readline())
      result.append('{}: {}'.format(fn, end - start))
    except:
      result.append('\n')

filenames = ['sub.rtt', 'event.rtt']
for fn in filenames:
  try:
    total = []
    with open(fn) as f:
      for line in f:
        total.append(int(line))
    result.append('{}: {}'.format(fn, sum(total)/len(total)))
  except:
    result.append('\n')

with open('event.size') as f:
  total = []
  try:
    for line in f:
      total.append(int(line))
    result.append('{}: {}'.format('event.size: ', sum(total)))
  except:
      result.append('\n')



print(result[0])
print(result[2])
print(result[1])
print(result[3])
print(result[4])
