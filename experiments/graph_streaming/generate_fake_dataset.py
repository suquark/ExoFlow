import random

SIZE = 2 ** 16

random.seed(42)

if __name__ == "__main__":
    for i in range(8):
        with open(f"chunk_{i}.txt", "w") as f:
            for _ in range(SIZE):
                a = random.randint(0, SIZE)
                b = random.randint(0, SIZE)
                while b == a:
                    b = random.randint(0, SIZE)
                f.write(f"{a} {b}\n")
