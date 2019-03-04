# ASORanking

Generates a ranking of ASOs (Autonomous Systems Organizations) based on normalized RUM (Real user measurement) performance metrics measured on Wikipedia visitors.

## Methodology

The ranking is done per country, separating mobile and desktop experiences.

### CPU microbenchmark

The backbone of this ranking is the normalization by CPU microbenchmark score.
We run that benchmark client-side for a short time on a separate [Worker](https://html.spec.whatwg.org/multipage/workers.html#worker) thread for sampled visitors, to avoid disrupting their experience.
It tells us how powerful their device is at the time of the measurement.
This is important, as on mobile devices memory usage and battery level can greatly influence the performance of the device at a given time.
In the wild we have observed wide ranges of CPU scores for the exact same phone models (as reported by their User Agent strings).

In addition to per-device differences, the device mix on different ASOs may vary greatly.
In order to compare ASOs fairly, we leverage the CPU microbenchmark.
For a given country, we compute the median CPU score for desktop and mobile, then for the rest of the ranking computations for
a country/platform combination, we only look at visitors whose CPU score is near the median for that combination.
This ensures that we look at scores for similarly powerful (median) devices across different ASOs, making the comparison fair between ASOs.

### Mobile vs desktop

We consider an ASO to be a mobile one if at least one recorded visitor from that ASO has a "cellular" ConnectionType, as reported by the [Network Information API](http://wicg.github.io/netinfo/).

Similarly, we consider an ASO to be a desktop one is at least one visitor has a "wifi" effectiveType. This means that some of the "desktop" ASOs in our
ranking are actually 3G/4G accessed over WiFi. This is expected, as a growing number of ISPs are selling 4G modems for home internet use.

When calculating the scores for mobile, we only keep pageviews to the mobile site.
When calculating scores for desktop, we only keep pageviews to the desktop site.

We acknowledge that some visitors access the mobile site on computers and some visitors access the desktop site on mobile devices.
The filtering of pageviews is not an attempt to compensate for those exceptions, but is merely there to keep the comparison of ASOs fair, as the median page weight is quite different on the mobile and desktop sites.

We wouldn't want an ASO corresponding to an ISP selling femtocell devices widely to be unfairly advantaged or disadvantaged because its mobile/desktop pageviews mix is different than other ASOs, resulting in lighter or heavier pages on average.

### RUM metrics

We report the medians for 2 core RUM metrics measures by the visitors' browsers: Time to first byte and Page Load Time, collected using the [Navigation Timing API](https://www.w3.org/TR/navigation-timing-2/).

Time to first byte (TTFB) is how long it takes between the client requesting the page and it receiving the first byte of data from us.
Page load time (PLT) is how long it takes to load the whole page, including all images and critical styles/scripts.

TTFB is the metric closest to latency, which is something ASOs might improve by peering with us or tweaking their routes to us.
PLT is the metric that correlates the most to the visitors' perception of performance, as shown by research we've conducted. It's what captures the ASOs' quality of service to their customers the best.

We also report the median transferSize as reported by the [Navigation Timing API](https://www.w3.org/TR/navigation-timing-2/) as a sanity check, to ensure that the RUM metrics comparison is fair between ASOs, and that the differences aren't caused by visitors using a particular ASO accessing much smaller or much bigger pages on average.

### Privacy

In order to respect the privacy of our visitors, we only report ASOs for which we aggregate more than 500 unique pageviews to generate scores.

## Usage

This script is meant to be run on a stat machine on cron on the 1st of every month, using the following syntax:

`python asoranking.py --publish`

Which will generate the ranking for the previous calendar month in the form
of a tsv file published to /srv/published-datasets/performance/autonomoussystems/

Which will in turn be picked up by the Performance Team website bot and
cross-posted to https://performance.wikimedia.org/asreport/