from starbase import Connection
import csv

def update(folder_name):
    print("Batch update... \n")
    print(folder_name)
    with open("/home/maria_dev/PresidentMoon-analysis/data/preprocessing/" + folder_name + "/clean_data.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            print(row[1])
            batch.update(folder_name, { 'date' : { row[1] : row[2] }}) # 1: date, 2: word


if __name__ == '__main__':
    c = Connection()
    twitter = c.table('twitter')
    # if (twitter.exists()):
    #     twitter.drop()
    twitter.create("date")

    batch = twitter.batch()

    if batch:
        # with open("/home/maria_dev/PresidentMoon-analysis/data/preprocessing/" + folder_name + "/clean_data.csv", "r", encoding="utf-8") as f:
        #     reader = csv.reader(f, delimiter=',')
        #     next(reader)
        #     for row in reader:
        #         print(row[1])
        #         batch.update(row[1], { "tweets": { "moon": row[2] }}) # 1: date, 2: word
        # update("moon")
        [update(folder_name) for folder_name in ["moon", "unification", "dprk"]]
        batch.commit(finalize=True)