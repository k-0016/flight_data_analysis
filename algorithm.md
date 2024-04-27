## On Time Flights Algorithm:

1. **Define Variable:**
   - Set `delayThreshold` to 10 minutes.

2. **Mapper Logic:**
   - **Input:** Each flight record.
   - **Process:**
     - Compute the sum of `ArrDelay` and `DepDelay`.
     - If the sum is less than `delayThreshold`, emit `(UniqueCarrier, 1)` indicating the flight is on schedule.
     - Otherwise, emit `(UniqueCarrier, 0)` indicating the flight is delayed.

3. **Reducer Logic:** 
   - **Input:** Key-value pairs from the Mapper where keys are airline carriers (`UniqueCarrier`) and values are 1 (on-time) or 0 (delayed).
   - **Process:**
     - Count the total number of values for each `UniqueCarrier` to get `totalCount`.
     - Count the number of values that are 1 (on-time) for each `UniqueCarrier`.
     - Compute the on-schedule probability for each airline as `onSchedule / totalCount`.
     - Store each `(probability, UniqueCarrier)` in an `ArrayList`.

4. **Final Steps:**
   - At context cleanup, sort the `ArrayList` in decreasing order of probabilities.
   - Write the sorted list of `(UniqueCarrier, probability)` pairs to HDFS.

## Average Taxi Time Algorithm

1. **Mapper Logic:**
   - **Input:** Each flight record.
   - **Process:**
     - Emit `(Origin, TaxiOut)` for the taxi time as the aircraft leaves the origin airport.
     - Emit `(Dest, TaxiIn)` for the taxi time as the aircraft arrives at the destination airport.

2. **Reducer Logic:**
   - **Input:** Key-value pairs from the Mapper where keys are airport codes and values are taxi times.
   - **Process:**
     - For each airport code, aggregate all taxi times and count the number of values to determine `totalCount`.
     - Compute the average taxi time for each airport by dividing the total taxi time by `totalCount`.
     - Store each `(avgTaxiTime, Airport)` in an `ArrayList`.

3. **Final Steps:**
   - At context cleanup, sort the `ArrayList` in decreasing order of average taxi times.
   - Write the sorted list of `(Airport, avgTaxiTime)` pairs to HDFS.

## Most Common Cancellation Cause Algorithm

1. **Mapper Logic:**
   - **Input:** Each flight record that indicates a cancellation (`Cancelled = 1`).
   - **Process:**
     - Emit `(CancellationCode, 1)` for each cancelled flight.

2. **Reducer Logic:**
   - **Input:** Key-value pairs from the Mapper where keys are cancellation codes and values are counts.
   - **Process:**
     - Sum all values for each cancellation code to get `totalCount` for that code.
     - Repeat this for each cancellation code received.

3. **Final Steps:**
   - Write each `(CancellationCode, totalCount)` to HDFS after processing all keys.

