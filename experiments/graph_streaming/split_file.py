import os

if __name__ == "__main__":
    fn = os.path.expanduser("~/efs/twitter_dataset/twitter-2010.txt")
    with open(fn) as f:
        files = []
        for i in range(8):
            files.append(open(f"chunk_{i}.txt", "w"))
        for i, line in enumerate(f):
            files[i % 8].write(line)
    for f in files:
        f.close()
