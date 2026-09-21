#!/bin/sh
set -eu
# Check exact covered/total statement counts, rather than a rounded percentage.
awk 'NR > 1 { total += $2; if ($3 > 0) covered += $2 }
END {
    if (total == 0) { print "No coverage statements found"; exit 1 }
    printf "Statement coverage: %d/%d (%.2f%%)\n", covered, total, 100 * covered / total
    if (covered != total) exit 1
}' "${1:-coverage.out}"
