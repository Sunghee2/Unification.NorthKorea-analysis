from starbase import Connection
import csv

c = Connection()
test = c.table('test')
if (ratings.exists()):
    ratings.drop()
test.create('test')

batch = test.batch()
if batch:
    print("Batch update... \n")
    with open("./data/result.csv", "r") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            batch.update(row[0], {'tweet': {row[1]: row[2]}})
    
    print("Committing...\n")
    batch.commit(finalize=True)

    print("Get ratings for users...\n")
    print(test.fetch("1"))

    print("\n")
    print(test.fetch("33"))