import os
import sys


def clean_csv(input_file, header, output_file):
    try:
        with open(input_file, "r", encoding="utf-8") as rf:
            with open(output_file, "w", encoding="utf-8") as wf:
                wf.write(header + "\n")
                while True:
                    line = rf.readline()
                    if not line:
                        break
                    if line.strip().endswith(","):
                        cleaned_line = line.strip()[:-1]
                        wf.write(cleaned_line + "\n")
                    else:
                        wf.write(line)
        print(f"done: {output_file}")
    except Exception as e:
        print(f"err: {e}")


if __name__ == "__main__":
    clean_csv(sys.argv[1], sys.argv[2], sys.argv[3])
