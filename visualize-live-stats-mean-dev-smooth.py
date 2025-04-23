import sys
import os
import glob
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import argparse
from scipy.interpolate import interp1d

# --- Helper to parse a single live-stats file ---
def parse_stats_file(filepath):
    details = {}
    inside_details = False
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line == '----- Details -----':
                inside_details = True
                continue
            if line.startswith('------ Summary -----'):
                break
            if inside_details and '=' in line:
                key, value = map(str.strip, line.split('=', 1))
                try:
                    value = float(value)
                except ValueError:
                    pass
                details[key] = value
    return details

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Plot mean±std coverage graphs for fuzzing runs.")
    parser.add_argument('main_dirs', nargs='+', help='List of main directories')
    parser.add_argument('--x', required=True, help='Variable name for x-axis')
    parser.add_argument('--y', required=True, help='Variable name for y-axis')
    parser.add_argument('--logx', action='store_true', help='Use logarithmic scale for x-axis')
    args = parser.parse_args()

    plt.figure(figsize=(10,6))

    for main_dir in args.main_dirs:
        label = os.path.basename(os.path.normpath(main_dir))

        runs = [os.path.join(main_dir, d) for d in sorted(os.listdir(main_dir)) if os.path.isdir(os.path.join(main_dir, d))]
        all_xs = []
        all_ys = []

        final_ys = []

        for run in runs:
            live_stats_dir = os.path.join(run, 'live-stats')
            if not os.path.isdir(live_stats_dir):
                continue

            data_points = [(1, 0)]
#             data_points = []
            for filepath in glob.glob(os.path.join(live_stats_dir, '*.txt')):
                details = parse_stats_file(filepath)
                if args.x in details and args.y in details:
                    data_points.append((details[args.x], details[args.y]))

            # Sort by x
            data_points.sort()
            if not data_points:
                print(f"no data points found for {run}")
                continue


            final_ys.append(data_points[-1][1])
            xs, ys = zip(*data_points)
            all_xs.append(np.array(xs))
            all_ys.append(np.array(ys))

        print(f"Final {args.y} values for {label}: {final_ys} Average={sum(final_ys)/len(final_ys)}")
        if not all_xs:
            continue

        print(f"Plotting {len(all_xs)} runs for directory '{main_dir}'")

        # Define a common x-grid
        min_x = min(x[0] for x in all_xs)
        max_x = max(x[-1] for x in all_xs)
        if args.logx:
            common_x = np.logspace(np.log10(min_x), np.log10(max_x), 1000)
        else:
            common_x = np.linspace(min_x, max_x, 1000)

        interpolated_ys = []
        for xs, ys in zip(all_xs, all_ys):
            f = interp1d(xs, ys, kind='linear', bounds_error=False, fill_value=(np.nan, ys[-1]))
            interpolated_ys.append(f(common_x))

        interpolated_ys = np.array(interpolated_ys)
        mean_y = np.nanmean(interpolated_ys, axis=0)
        std_y = np.nanstd(interpolated_ys, axis=0)


        plt.plot(common_x, mean_y, label=label)
        plt.fill_between(common_x, mean_y - std_y, mean_y + std_y, alpha=0.2)

    plt.xlabel(args.x)
    plt.ylabel(args.y)
    plt.title(f"{args.y} vs {args.x}")
    plt.legend()
    plt.grid(True)

    if args.logx:
        plt.xscale('log')

    os.makedirs('graphs', exist_ok=True)
    import time
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    plt.savefig(f"graphs/graph_{timestamp}.pdf")
    print(f"Saved plot as graphs/graph_{timestamp}.png")
    plt.show()

if __name__ == "__main__":
    main()
