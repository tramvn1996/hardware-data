# 2 facts to replicate from the Backblaze Hard Drive Stats for 2017 report:
- 81.76 — The number of hard drives that were installed each day in 2017. 
	This includes new drives, migrations, and failure replacements.
- 4.13 — The average number of hard drives that have failed each day in 2017.

# Results:
- The number of hard drives that were installed each day in 2017: 53.055
- The average number of hard drives that have failed each day in 2017 : 4.41

# Methodology:
- I mostly work with column "serial_number" and "failure"
-- serial_number: distinct number of each hard drives for each day
-- failure: 1 if it failed that day, and 0 if it works fine
- To calculate the failure, I just add up the 1's in failure column and average them
- To calculate the number of hard drives, I add up the 0's, then take the difference between
	2 consecutive days, then average the difference

# Conclusion:
- The average number of hard drives that failed is consistent with the 
	website statistics
- The number of hard drives, on the other hand, are a little off. I suspect
	this might happen because I didn't filter out when the difference number
	of hard drives between days are negative.
	-- To test this, I filtered out the negative differences, but the avg turned out 
	tobe about 400, which is way off. So I'll do some further investigation into why it's not consistent.
- I ran this on all the 2017 datasets, and it took about 163 seconds (almost 3 mins)
	to run
