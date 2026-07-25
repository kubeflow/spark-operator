#!/bin/bash
# Script to add assigned-* labels to all open issues and PRs
# Most frequent contributor from past 100 commits: RobuRishabh
# Run with: bash scripts/assign-labels.sh

OWNER="kubeflow"
REPO="spark-operator"

# All open issue/PR numbers (collected on 2026-07-25)
ITEMS=(
  3054 3053 3052 3051 3049 3047 3045 3044 3042 3041
  3040 3038 3037 3036 3035 3034 3033 3031 3026 3025
  3023 3019 3016 3013 3006 3005 3003 3002 2997 2996
  2994 2992 2991 2989 2987 2985 2984 2983 2977 2972
  2967 2964 2963 2962 2961 2960 2959 2958 2957 2953
  2949 2943 2935 2934 2932 2928 2927 2923 2921 2920
  2916 2913 2907 2906 2905 2904 2901 2900 2897 2896
  2880 2879 2866 2858 2857 2851 2842 2831 2811 2790
  2788 2781 2744 2715 2669 2583 2558 2526 2510 2378
  2301 2297 2288 2284 2279 2244 2193 2180 2139 2130
)

echo "Adding label assigned-roburishabh to all open issues and PRs..."
for item in "${ITEMS[@]}"; do
  echo "Processing #$item..."
  gh issue edit "$item" --add-label "assigned-roburishabh" -R "$OWNER/$REPO" 2>&1 || true
done

echo "Done!"
