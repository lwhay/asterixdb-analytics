/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.kmeans;

import java.io.Serializable;

/**
 * IMRU model which will be used in map() and updated in update()
 */
public class KMeansModel implements Serializable {
    Centroid[] centroids;
    public int roundsRemaining = 20;
    public double lastDistanceSum = 0;

    public KMeansModel(int k, int roundsRemaining) {
        this.roundsRemaining = roundsRemaining;
        centroids = new Centroid[k];
        for (int i = 0; i < k; i++)
            centroids[i] = new Centroid();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < centroids.length; i++)
            sb.append(centroids[i] + " ");
        sb.append(roundsRemaining);
        return sb.toString();
    }
}
