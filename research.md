# Notes on the math behind finding segment boundaries

I wrote these down while working on the segment finder because I kept going back to the same handful of ideas, some of which I first ran into years ago and only half remembered. Putting them in one place helped me understand why the tool behaves the way it does, and why some approaches that look faster on paper aren't.

## What the problem actually is

If you sort a collection by `_id` and want N equal segments, the boundaries are just the `_id` values at positions n/N, 2n/N, and so on up to (N-1)n/N. In statistics those are the N-quantiles. In algorithms it's called the selection problem: find the k-th smallest item.

The cleanest way I found to think about it is with the CDF. Let F(x) be the fraction of documents whose `_id` is at most x. Then boundary k is

$$b_k = F^{-1}\left(\frac{k}{N}\right)$$

Every method I looked at is really just a different way of getting F, or a guess at it:

- Walking the index with one cursor counts every key, so you get F exactly. Costs one full scan.
- Using `skip()` also counts every key, just on the server side inside each skip call. Still a full scan.
- Interpolating between the first and last `_id` assumes F is a straight line. Two index lookups, basically free, but only as good as that assumption.
- Sampling estimates F from a random subset. Cost depends on the sample size, not the collection size.

## Why I couldn't get around the full scan

My first instinct was some kind of binary search. That doesn't work here, and the reason is worth knowing.

Binary search needs to know a key's rank, meaning how many keys come before it. A B-tree index (Bayer and McCreight, 1972) can find a key in O(log n) steps, but it doesn't keep track of how many keys sit under each node. So it can tell you where a key is, but not what position it's at, unless you walk the leaves and count.

There's a structure that fixes this, the order-statistic tree from CLRS (*Introduction to Algorithms*, chapter 14). It stores subtree sizes, so you can jump straight to the key at any rank in O(log n). If DocumentDB indexes worked like that, all the boundaries would cost O(N log n) instead of a full pass. They don't, so counting is O(n) no matter what. All I can control is how fast the scan goes, whether it's split up, and how much it hurts the cluster.

This is also why the skip approach was never really faster for us. `skip(s)` still walks s index entries on the server. The tool skips from the previous boundary each time, so the total is still about n. It's just packed into a few really long server calls, and on a 1.2 billion document collection those kept timing out with nothing saved.

## Selection algorithms, and why they don't apply directly

The classic selection algorithms are Hoare's quickselect (1961, "Algorithm 65: Find"), which is O(n) on average, and median of medians (Blum, Floyd, Pratt, Rivest and Tarjan, 1973), which is O(n) worst case. Both assume you can move data around, which you can't with an index.

The closer match is Munro and Paterson (1980, "Selection and sorting with limited storage"). They looked at selection when you can only read data in order, a few passes at a time, with little memory. With p passes you need roughly n^(1/p) memory to get an exact answer. With one pass and a small amount of memory, you can only get approximate answers. That trade-off is exactly what the marker approach below makes.

## Interpolation, and the Gauss thing I remembered

Interpolation search guesses where a key is by assuming keys are spread evenly between the two ends:

$$\text{position} \approx \text{lo} + (\text{hi} - \text{lo}) \cdot \frac{x - x_{\text{lo}}}{x_{\text{hi}} - x_{\text{lo}}}$$

Peterson described it in 1957 ("Addressing for random-access storage," *IBM Journal of Research and Development*). Perl, Itai and Avni proved in 1978 that it averages O(log log n) probes on uniformly spread keys ("Interpolation search: a log log N search," *CACM*), and Yao and Yao (1976) showed you can't do better on that kind of data. On skewed data it can get as bad as O(n).

The parallel mode uses the same idea backwards. Instead of finding a key's position, it picks evenly spaced key values and hopes they're roughly evenly spaced in position too. For ObjectIds the first 4 bytes are a timestamp, so I split by time: `ObjectId.from_datetime(t0 + (t1 - t0) * i / R)`. For integer ids it's just evenly spaced numbers. Either way it's a straight line drawn between the two points of F⁻¹ I actually know, the first `_id` and the last.

The Gauss connection I had in my head from way back turned out to be one of two things, and I think both are relevant.

The first is his interpolation formulas. Gauss wrote "Theoria interpolationis methodo nova tractata" around 1805. It wasn't published until after he died, in volume 3 of his collected *Werke* (1866). It has the Gauss forward and backward interpolation formulas, which build on Newton's divided differences to estimate a function between known points. The straight-line split the tool uses is the first-order version. If you knew a few more points of F, say from a small sample, you could fit a better curve through them, and Gauss's formulas are one way to do that.

The second is the normal distribution. In *Theoria motus corporum coelestium* (1809) he justified least squares with the error law we now call Gaussian. That shows up the moment you estimate boundaries from a sample instead of counting.

## What sampling would buy

If you take a random sample of size s, the estimated quantiles are close to normally distributed for large s. For one boundary at level p, the variance works out to

$$\frac{p(1-p)}{s \, f(F^{-1}(p))^2}$$

where f is how dense the keys are at that point (Bahadur, 1966, "A note on quantiles in large samples"). In terms of position, the standard error is about √(p(1-p)/s) as a fraction of n. What surprised me is that n doesn't appear at all.

For all the boundaries at once there's the Dvoretzky-Kiefer-Wolfowitz inequality (1956), with the tight constant from Massart (1990):

$$P\left(\sup_x |F_s(x) - F(x)| > \varepsilon\right) \le 2e^{-2s\varepsilon^2}$$

Solving for s gives the sample size to keep every boundary within ε·n positions of the real one, with probability 1 - δ:

$$s \ge \frac{\ln(2/\delta)}{2\varepsilon^2}$$

With δ = 1%, ε = 1% needs about 26,500 documents, ε = 0.5% needs about 106,000, and ε = 0.1% needs about 2.65 million. For 8 segments, 1% of n is 8% of a segment, so roughly 26,500 sampled documents would already give usable boundaries on any size collection.

The reason I didn't go this route is that it depends on getting a truly random sample cheaply. MongoDB's `$sample` uses a random cursor when the sample is small relative to the collection, but I don't know how DocumentDB handles it on a billion documents, and I didn't want to find out on production. Counting is slower but I know exactly what it does.

## The marker trick

Each worker writes down its running count and the current `_id` every m documents, plus the first document in its range. When everything's done, the ranges get stitched together in `_id` order with running offsets, which gives a sorted list of (position, `_id`) pairs no more than m apart. For each target position kn/N, I take the first marker at or past it.

That makes the error easy to bound. The chosen marker is never before the target and is less than m positions after it:

$$0 \le \text{rank}(\hat{b}_k) - \frac{kn}{N} < m$$

I set m to max(100, n / (1000N)), which keeps each boundary within 0.1% of a segment. It only needs about 1000N markers, so 8,000 for 8 segments, no matter how big the collection is.

There are smarter one-pass summaries out there. Greenwald and Khanna (2001, "Space-efficient online computation of quantile summaries," SIGMOD) handle unknown n with error guarantees. Dunning's t-digest is very good near the extremes. I didn't need either: n is known up front from `collStats`, the data comes in sorted, and I only care about evenly spaced boundaries, so fixed spacing does the job.

On my test data (MongoDB 7.0, 3-member replica set):

- 1 million ObjectIds with 70% of them packed into the first 10% of the time range, 8 segments: worst boundary was off by 73 documents, 0.06% of a segment.
- 200,000 integer ids with random gaps, 4 segments: worst was 95 documents, 0.19% of a segment. It's higher because m bottoms out at 100 on small collections.

## Being off doesn't break anything

This took me a minute to appreciate. DMS only needs the boundaries in increasing order. Any increasing list b₁ < b₂ < ... < b_(N-1) splits the key space into ranges that don't overlap and cover everything:

$$(-\infty, b_1), \; [b_1, b_2), \; \dots, \; [b_{N-1}, +\infty)$$

So being a bit off only makes the segments a little uneven. It never causes data to be skipped. Same reason documents inserted during the scan don't matter: they fall into some range and DMS picks them up.

## Keeping the workers busy

Interpolated ranges come out uneven whenever F isn't a straight line, which in real data is basically always. To soften that I cut the key space into 4 ranges per worker and hand them out from a queue as workers free up. Graham worked this out in 1966 ("Bounds for certain multiprocessing anomalies," *Bell System Technical Journal*). With total work n, W workers, and the biggest single range p_max, everything finishes by

$$T \le \frac{n}{W} + p_{\max}$$

If the keys are spread evenly, p_max is about n/(4W), so you're at most 25% slower than a perfect split. If they're skewed, p_max can be huge. In my skewed test set, the first of 16 time ranges held about 45% of all the documents, which caps the speedup around 2.2x however many workers you throw at it.

Parallel databases ran into the same thing with range partitioning. DeWitt, Naughton, Schneider and Seshadri (1992, "Practical skew handling in parallel joins," VLDB) dealt with it by sampling to pick split points. The two improvements I'd make if this ever matters:

1. Pick split points from a small sample instead of interpolating. With about 26,500 samples the ranges would come out within about 1% of each other.
2. When a worker runs out of ranges, split the biggest unfinished one in half and give it the second half. That's work stealing. Polychronopoulos and Kuck's guided self-scheduling (1987, *IEEE Transactions on Computers*) is the classic version, handing out shrinking chunks.

## Not hurting the database

All the workers share one token bucket with rate r and room for r tokens. In any window of t seconds, total reads stay under

$$D(t) \le r \cdot t + b$$

That's the standard token bucket guarantee from network traffic shaping, related to Turner's leaky bucket (1986, "New directions in communications," *IEEE Communications Magazine*). In practice it means the replica never sees more than about one extra second's worth of reads above the limit, however many workers are running.

How long the scan takes is whichever is slower, the limit or what the workers can actually pull:

$$T \approx \frac{n}{\min(r, \; W v)}$$

For 1.23 billion documents that's about 34 hours at 10,000 docs/sec, 17 hours at the 20,000 default, and 7 hours at 50,000. Skew makes it longer. I'd set the limit based on how much headroom the replica has in CloudWatch, not on how fast I want it done.

## References

- Bahadur, R. R. (1966). A note on quantiles in large samples. *Annals of Mathematical Statistics*, 37(3).
- Bayer, R. and McCreight, E. (1972). Organization and maintenance of large ordered indexes. *Acta Informatica*, 1.
- Blum, M., Floyd, R., Pratt, V., Rivest, R. and Tarjan, R. (1973). Time bounds for selection. *Journal of Computer and System Sciences*, 7(4).
- Cormen, T., Leiserson, C., Rivest, R. and Stein, C. *Introduction to Algorithms*, 3rd edition, chapter 14.
- DeWitt, D., Naughton, J., Schneider, D. and Seshadri, S. (1992). Practical skew handling in parallel joins. *VLDB*.
- Dvoretzky, A., Kiefer, J. and Wolfowitz, J. (1956). Asymptotic minimax character of the sample distribution function. *Annals of Mathematical Statistics*, 27(3).
- Gauss, C. F. (1809). *Theoria motus corporum coelestium*.
- Gauss, C. F. Theoria interpolationis methodo nova tractata. *Werke*, volume 3 (1866).
- Graham, R. L. (1966). Bounds for certain multiprocessing anomalies. *Bell System Technical Journal*, 45(9).
- Greenwald, M. and Khanna, S. (2001). Space-efficient online computation of quantile summaries. *SIGMOD*.
- Hoare, C. A. R. (1961). Algorithm 65: Find. *Communications of the ACM*, 4(7).
- Massart, P. (1990). The tight constant in the Dvoretzky-Kiefer-Wolfowitz inequality. *Annals of Probability*, 18(3).
- Munro, J. I. and Paterson, M. S. (1980). Selection and sorting with limited storage. *Theoretical Computer Science*, 12(3).
- Perl, Y., Itai, A. and Avni, H. (1978). Interpolation search: a log log N search. *Communications of the ACM*, 21(7).
- Peterson, W. W. (1957). Addressing for random-access storage. *IBM Journal of Research and Development*, 1(2).
- Polychronopoulos, C. and Kuck, D. (1987). Guided self-scheduling: a practical scheduling scheme for parallel supercomputers. *IEEE Transactions on Computers*, C-36(12).
- Turner, J. (1986). New directions in communications (or which way to the information age?). *IEEE Communications Magazine*, 24(10).
- Yao, A. C. and Yao, F. F. (1976). The complexity of searching an ordered random table. *FOCS*.
