# mongo_load_generator

This script generates loads to show the benefits of workload isolation.

Create two clusters; a normal one and one with a read-only replica.
NOTE: This script is tested with M10 on AWS. Load may not be as apparant with larger clusters.

Run the script with the 'INIT' command on both clusters. This can take a while so do it before you want to demo it.

Open a window and run the script with 'OPERATE' to generate a baseline operational workload.

Then run the script with 'ANALYZE' to see the effects of high volume reads on performance.

Repeate the previous two steps on the cluster with the read-only replica to show how workload isolation will save you performance headaches.
