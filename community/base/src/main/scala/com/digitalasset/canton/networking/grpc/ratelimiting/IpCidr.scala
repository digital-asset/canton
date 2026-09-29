// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.google.common.net.InetAddresses

import java.net.InetAddress
import scala.annotation.tailrec
import scala.util.Try

/** An IP network in CIDR notation, e.g. `10.0.0.0/8` or `2001:db8::/32`. A value without a prefix
  * length denotes a single host, i.e. a `/32` (IPv4) or `/128` (IPv6) network.
  *
  * The address is stored as a pair of `Long`s holding the raw address bytes left-aligned in a 128
  * bit big-endian word, with all host bits already zeroed out. This keeps [[IpCidr.contains]] -
  * which runs on every request against every configured network - to a handful of allocation-free
  * integer operations, and works uniformly for IPv4 (4 bytes) and IPv6 (16 bytes).
  *
  * Ported from splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/IpCidrRateLimits.scala`
  * (`IpCidr` is defined there, alongside `IpCidrRateLimits`), split out into its own file here.
  *
  * @param high
  *   bytes 0-7 of the network address.
  * @param low
  *   bytes 8-15 of the network address, always 0 for IPv4.
  * @param prefixLength
  *   the number of significant bits, between 0 and `addressBits`.
  * @param addressBits
  *   32 for IPv4 and 128 for IPv6.
  */
final case class IpCidr(high: Long, low: Long, prefixLength: Int, addressBits: Int) {

  private val maskHigh: Long = IpCidr.mask64(prefixLength)
  private val maskLow: Long = IpCidr.mask64(prefixLength - 64)

  /** Whether this network fully contains `other`, which is the case if both are of the same IP
    * version, this network is not more specific than `other` and their common prefix matches.
    */
  def contains(other: IpCidr): Boolean =
    addressBits == other.addressBits &&
      prefixLength <= other.prefixLength &&
      (other.high & maskHigh) == high &&
      (other.low & maskLow) == low

  override def toString: String = {
    val bytes = Array.tabulate(addressBits / 8) { index =>
      (if (index < 8) high >>> (56 - 8 * index) else low >>> (56 - 8 * (index - 8))).toByte
    }
    s"${InetAddress.getByAddress(bytes).getHostAddress}/$prefixLength"
  }
}

object IpCidr {

  def tryParse(value: String): IpCidr =
    parse(value).getOrElse(
      throw new IllegalArgumentException(
        s"'$value' is not a valid IP address or network in CIDR notation"
      )
    )

  def parse(value: String): Option[IpCidr] =
    value.split("/", -1) match {
      case Array(address) =>
        ofAddress(address).map { case (high, low, addressBits) =>
          IpCidr(high, low, addressBits, addressBits)
        }
      case Array(address, prefixLength) =>
        ofAddress(address).flatMap { case (high, low, addressBits) =>
          Try(prefixLength.trim.toInt).toOption
            .filter(length => length >= 0 && length <= addressBits)
            .map(length =>
              // zero out the host bits so that `contains` reduces to an equality check
              IpCidr(high & mask64(length), low & mask64(length - 64), length, addressBits)
            )
        }
      case _ => None
    }

  /** Decodes an IP literal into `(high, low, addressBits)`. */
  private def ofAddress(address: String): Option[(Long, Long, Int)] = {
    val trimmed = address.trim
    // must not do a DNS lookup, only IP literals are accepted
    Option
      .when(InetAddresses.isInetAddress(trimmed))(InetAddresses.forString(trimmed))
      // IPv4-mapped IPv6 addresses (e.g. ::ffff:192.0.2.1) are converted to their IPv4 address by
      // InetAddresses.forString, so they match the same networks as the plain IPv4 address
      .map { inetAddress =>
        val bytes = inetAddress.getAddress
        val (high, low) = pack(bytes, 0, 0L, 0L)
        (high, low, bytes.length * 8)
      }
  }

  /** Packs the big-endian address `bytes` left-aligned into a 128 bit `(high, low)` word. */
  @tailrec
  private def pack(bytes: Array[Byte], index: Int, high: Long, low: Long): (Long, Long) =
    if (index >= bytes.length) (high, low)
    else {
      val byte = bytes(index) & 0xffL
      if (index < 8) pack(bytes, index + 1, high | (byte << (56 - 8 * index)), low)
      else pack(bytes, index + 1, high, low | (byte << (56 - 8 * (index - 8))))
    }

  /** A big-endian mask keeping the leading `bits` bits, clamped to `[0, 64]`. */
  private[ratelimiting] def mask64(bits: Int): Long =
    if (bits <= 0) 0L
    else if (bits >= 64) -1L
    else -1L << (64 - bits)
}
