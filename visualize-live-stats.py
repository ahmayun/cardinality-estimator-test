import os
import sys
import matplotlib.pyplot as plt
import time

def parse_details(filepath):
    details = {}
    with open(filepath, 'r') as f:
        lines = f.readlines()
        in_details = False
        for line in lines:
            line = line.strip()
            if line == '----- Details -----':
                in_details = True
                continue
            if line.startswith('------') or line.startswith('===='):
                in_details = False
            if in_details and '=' in line:
                key, value = line.split('=', 1)
                details[key.strip()] = value.strip()
    return details

def collect_points(dir_path, x_key, y_key):
    points = []
    for filename in os.listdir(dir_path):
        if filename.endswith('.txt'):
            filepath = os.path.join(dir_path, filename)
            details = parse_details(filepath)
            if x_key in details and y_key in details:
                try:
                    x = float(details[x_key])
                    y = float(details[y_key])
                    points.append((x, y))
                except ValueError:
                    continue
    points.sort()
    return zip(*points) if points else ([], [])

def get_parent_dir_name(path):
    return os.path.basename(os.path.dirname(path))

def main():
    if len(sys.argv) < 5:
        print(f"Usage: {sys.argv[0]} <directory1> <directory2> <x_key> <y_key> [--logx]")
        sys.exit(1)

    dir_path1 = sys.argv[1]
    dir_path2 = sys.argv[2]
    x_key = sys.argv[3]
    y_key = sys.argv[4]
    logx = '--logx' in sys.argv

    xs1, ys1 = collect_points(dir_path1, x_key, y_key)
    xs2, ys2 = collect_points(dir_path2, x_key, y_key)

    if not xs1 and not xs2:
        print("No valid data points found in either directory.")
        sys.exit(1)

    plt.figure()
    if logx:
        plt.xscale('log')
    if xs1 and ys1:
        plt.plot(xs1, ys1, label=get_parent_dir_name(dir_path1))
    if xs2 and ys2:
        plt.plot(xs2, ys2, label=get_parent_dir_name(dir_path2))
    plt.xlabel(x_key)
    plt.ylabel(y_key)
    plt.title(f"{y_key} vs {x_key}")
    plt.grid(True)
    plt.legend()

    os.makedirs('graphs', exist_ok=True)
    timestamp = int(time.time())
    save_path = os.path.join('graphs', f'graph_{timestamp}.png')
    plt.savefig(save_path)
    print(f"Graph saved to {save_path}")

    plt.show()

if __name__ == "__main__":
    main()