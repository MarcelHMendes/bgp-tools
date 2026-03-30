import json
import sys

def compare_json(file1, file2):
    with open(file1, "r", encoding="utf-8") as f1:
        data1 = json.load(f1)["vtrmiami"]

    with open(file2, "r", encoding="utf-8") as f2:
        data2 = json.load(f2)["vtrmiami"]

    diff_count = 0

    for key in data1:
        if key in data2:
            v1 = str(data1[key]).strip()
            v2 = str(data2[key]).strip()

            if v1 != v2:
                diff_count += 1
                print(f'diferença: "{key}": "{data1[key]}" - "{key}": "{data2[key]}"')

    print(f"\nTotal de diferenças: {diff_count}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python compare_json.py <arquivo1.json> <arquivo2.json>")
        sys.exit(1)

    compare_json(sys.argv[1], sys.argv[2])
