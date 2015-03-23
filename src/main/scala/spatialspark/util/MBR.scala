/*
 * Copyright 2015 Simin You
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package spatialspark.util

import com.vividsolutions.jts.geom.Envelope

/**
 * Created by Simin You on 10/22/14.
 */
class MBR (val xmin:Double,
           val ymin:Double,
           val xmax:Double,
           val ymax:Double) extends Serializable{
  def union(b:MBR):MBR = {
    new MBR(this.xmin min b.xmin, this.ymin min b.ymin, this.xmax max b.xmax, this.ymax max b.ymax)
  }

  def intersects(b:MBR):Boolean = {
    !(this.xmin > b.xmax || this.xmax < b.xmin || this.ymin > b.ymax || this.ymax < b.ymin)
  }

  def center():(Double, Double) = {
    ((xmin + xmax) / 2, (ymin + ymax) / 2)
  }

  override def toString() = {
    xmin + "," + ymin + "," + xmax + "," + ymax;
  }


  def toString(separator:String) = {
    xmin + separator + ymin + separator + xmax + separator + ymax;
  }

  def toText() = {
    val result = s"POLYGON(($xmin $ymin,$xmax $ymin,$xmax $ymax,$xmin $ymax,$xmin $ymin))"
    result
  }

  def toEnvelop():Envelope = {
    new Envelope(xmin, xmax, ymin, ymax)
  }
}

