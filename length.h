/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The length/string/bool/char wire codec cluster uses for its own Raft and
// bus message framing now lives in its own library, github.com/Kronuz/varint
// (extracted from this exact code -- see its README's "Provenance" section
// for why, including a real licensing issue found in Xapiand's original
// length.h/length.cc that this code was NEVER actually derived from, despite
// the similar name). This header is now a thin compatibility shim: every
// call site in this repo uses these names unqualified from inside
// `namespace cluster`, so re-exporting them here via `using` keeps every
// existing #include "length.h" and call site working unchanged.

#pragma once

#include <varint.hh>

namespace cluster {

using varint::serialise_length;
using varint::unserialise_length;
using varint::serialise_string;
using varint::unserialise_string;
using varint::serialise_bool;
using varint::unserialise_bool;
using varint::serialise_char;
using varint::unserialise_char;

} // namespace cluster
