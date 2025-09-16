import os
import sys

def parse_test_cases(file_content):
    """Parses test cases from a file's content."""
    test_cases = []
    # Split by ---START---. The first element is the content before the first ---START---, which we can ignore.
    raw_cases = file_content.split("---START---\n")
    if len(raw_cases) > 0:
        # The first element is the content before the first ---START---
        raw_cases = raw_cases[1:]

    for raw_case in raw_cases:
        if "---END---\n" in raw_case:
            case_content = raw_case.split("---END---\n")[0]
            test_cases.append(case_content.strip())
            
    return test_cases

def does_pass(groundtruth_case, result_case):
    """Checks if a result case passes against the groundtruth case."""
    # If both have errors, they are considered matched.
    if "ERROR:" in groundtruth_case and "ERROR:" in result_case:
        return True
    # Otherwise, they must be identical.
    return groundtruth_case == result_case

def compare_and_report(groundtruth_path, results_a_path, results_b_path, filename):
    """Compares two result sets (A and B) against a groundtruth and reports differences."""
    with open(groundtruth_path, 'r') as f:
        groundtruth_content = f.read()
    with open(results_a_path, 'r') as f:
        results_a_content = f.read()
    with open(results_b_path, 'r') as f:
        results_b_content = f.read()

    groundtruth_cases = parse_test_cases(groundtruth_content)
    a_cases = parse_test_cases(results_a_content)
    b_cases = parse_test_cases(results_b_content)

    num_cases = min(len(groundtruth_cases), len(a_cases), len(b_cases))

    improvements = []
    for i in range(num_cases):
        groundtruth_case = groundtruth_cases[i]
        a_case = a_cases[i]
        b_case = b_cases[i]

        a_passes = does_pass(groundtruth_case, a_case)
        b_passes = does_pass(groundtruth_case, b_case)

        if not a_passes and b_passes:
            improvements.append({
                "case_number": i + 1,
                "a_output": a_case,
                "b_output": b_case
            })

    if improvements:
        print(f"--- {filename} ---")
        for improvement in improvements:
            print(f"\n## Case {improvement['case_number']}: A fails, B passes\n")
            print("--- A's output ---")
            print(improvement['a_output'])
            print("\n--- B's output ---")
            print(improvement['b_output'])
            print("\n" + "="*40 + "\n")

def main():
    """Main function to compare three sets of test results."""
    if len(sys.argv) != 4:
        print("Usage: python compare_ab.py <groundtruth_dir> <results_a_dir> <results_b_dir>")
        sys.exit(1)

    groundtruth_dir = sys.argv[1]
    results_a_dir = sys.argv[2]
    results_b_dir = sys.argv[3]

    try:
        groundtruth_files = set(os.listdir(groundtruth_dir))
        results_a_files = set(os.listdir(results_a_dir))
        results_b_files = set(os.listdir(results_b_dir))
    except FileNotFoundError as e:
        print(f"Error: Directory not found - {e.filename}")
        sys.exit(1)

    common_files = sorted(list(groundtruth_files.intersection(results_a_files).intersection(results_b_files)))

    if not common_files:
        print("No common files found across the three directories.")
        return

    for filename in common_files:
        groundtruth_path = os.path.join(groundtruth_dir, filename)
        results_a_path = os.path.join(results_a_dir, filename)
        results_b_path = os.path.join(results_b_dir, filename)

        compare_and_report(groundtruth_path, results_a_path, results_b_path, filename)

if __name__ == "__main__":
    main()
