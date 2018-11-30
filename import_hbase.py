from starbase import Connection
import csv

c = Connection()
test = c.table('test')
if (test.exists()):
    test.drop()
test.create('test')

batch = test.batch()
if batch:
    print("Batch update... \n")
    with open("./data/result.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            print(row[32])
            batch.update(row[0], {'tweets': {'문재인': row[20]}}) # 32:date, 20: hastags, 35: word
    
    print("Committing...\n")
    batch.commit(finalize=True)

    print("Get ratings for users...\n")
    print(test.fetch("1"))

    print("\n")
    print(test.fetch("2"))