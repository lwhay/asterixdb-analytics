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

public class Centroid implements Serializable {
    double x, y;
    int count = 0;

    public double dis(DataPoint dp) {
        return Math.sqrt((x - dp.x) * (x - dp.x) + (y - dp.y) * (y - dp.y));
    }

    public void add(DataPoint dp) {
        x += dp.x;
        y += dp.y;
        count++;
    }

    public void add(Centroid c) {
        x += c.x;
        y += c.y;
        count += c.count;
    }

    public boolean set(Centroid c) {
        double x = c.x;
        double y = c.y;
        if (c.count > 0) {
            x /= c.count;
            y /= c.count;
        }
        if (Math.abs(x - this.x) < 1E-10 && Math.abs(y - this.y) < 1E-10)
            return false;
        this.x = x;
        this.y = y;
        return true;
    }

    public void set(DataPoint p) {
        x = p.x;
        y = p.y;
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
