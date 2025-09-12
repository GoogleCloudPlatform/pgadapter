import json
import os
import sys


def parse_test_cases(file_content):
    """Parses test cases from a file's content."""
    test_cases = []
    in_test_case = False
    current_test_case = []
    for line in file_content.splitlines():
        if line.strip() == "---START---":
            in_test_case = True
            current_test_case = []
        elif line.strip() == "---END---":
            if in_test_case:
                test_cases.append("\n".join(current_test_case))
                in_test_case = False
        elif in_test_case:
            current_test_case.append(line)
    return test_cases


def compare_files(groundtruth_path, run_results_path):
    """Compares test cases in two files and returns passed and total counts."""
    with open(groundtruth_path, 'r') as f:
        groundtruth_content = f.read()
    with open(run_results_path, 'r') as f:
        run_results_content = f.read()

    groundtruth_cases = parse_test_cases(groundtruth_content)
    run_results_cases = parse_test_cases(run_results_content)

    passed_count = 0
    total_count = len(groundtruth_cases)

    for i in range(min(len(groundtruth_cases), len(run_results_cases))):
        if groundtruth_cases[i].find("ERROR:") != -1 and run_results_cases[i].find("ERROR:") != -1:
            # if both has errors, then we consider them as matched
            passed_count += 1
        elif groundtruth_cases[i] == run_results_cases[i]:
            passed_count += 1
        # if groundtruth_cases[i] == run_results_cases[i]:
        #    passed_count += 1

    if passed_count != total_count:
        return passed_count, total_count, groundtruth_content, run_results_content
    else:
        return passed_count, total_count, None, None

def main():
    """Main function to compare test results."""
    if len(sys.argv) != 3:
        print("Usage: python compare_results.py <groundtruth_dir> <run_results_dir>")
        sys.exit(1)

    groundtruth_dir = sys.argv[1]
    run_results_dir = sys.argv[2]
    json_output_path = "results.json"

    groundtruth_files = set(os.listdir(groundtruth_dir))
    run_results_files = set(os.listdir(run_results_dir))

    common_files = sorted(list(groundtruth_files.intersection(run_results_files)))

    total_passed_all = 0
    total_tests_all = 0

    print("--- Test Summary ---")

    results_map = {}

    for filename in common_files:
        groundtruth_path = os.path.join(groundtruth_dir, filename)
        run_results_path = os.path.join(run_results_dir, filename)

        passed, total, groundtruth_content, run_results_content = compare_files(groundtruth_path, run_results_path)
        total_passed_all += passed
        total_tests_all += total

        print(f"{filename}: {passed}/{total} passed")
        results_map[filename] = {"passed": passed, "total": total}

    print("\n--- Overall Summary ---")
    print(f"Total Passed: {total_passed_all}/{total_tests_all}")

    # Write results to a json file
    with open(json_output_path, "w") as json_file:
        json.dump(
            {
                "overall": {
                    "passed": total_passed_all,
                    "total": total_tests_all
                },
                "files": results_map
            },
            json_file,
            indent=4)

if __name__ == "__main__":
    main()
