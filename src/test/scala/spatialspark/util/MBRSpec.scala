/*
 * Copyright 2015 Kamil Gorlo
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

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.scalatest.{Matchers, FlatSpec}

class MBRSpec extends FlatSpec with Matchers {

  val Mbr = MBR(-1, 0, 5, 7)

  behavior of "MBR case class"

  it should "calculate center correctly" in {
    Mbr.center should be ((2, 3.5))
  }

  it should "check intersection correctly" in {
    Mbr.intersects(Mbr) should be (true)
    Mbr.intersects(MBR(0, 0, 1, 1)) should be (true)
    Mbr.intersects(MBR(4, 4, 6, 6)) should be (true)
    Mbr.intersects(MBR(5, 7, 10, 10)) should be (true)
    Mbr.intersects(MBR(6, 6, 10, 10)) should be (false)
  }

  it should "union with other mbr correctly" in {
    val bigMbr = MBR(-20, -20, 20, 20)

    Mbr.union(Mbr) should be (Mbr)
    Mbr.union(MBR(0, 0, 1, 1)) should be (Mbr)
    Mbr.union(MBR(5, 7, 10, 10)) should be (MBR(-1, 0, 10, 10))
    Mbr.union(bigMbr) should be (bigMbr)
  }

  it should "have correct WKT representation" in {
    val geometry: Geometry = new WKTReader().read(Mbr.toText)
    val geometryEnvelope: Envelope = geometry.getEnvelopeInternal

    geometry should be a 'rectangle
    Mbr.toEnvelope should be (geometryEnvelope)
  }

  it should "generate correct string with separator" in {
    Mbr.toString("\t") should be ("-1.0\t0.0\t5.0\t7.0")
  }
}
