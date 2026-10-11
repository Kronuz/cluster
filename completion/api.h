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

#pragma once
#include "operation.h"
#include "posix.h"
#include "completion.h"

// Keep the existing canonical types for source and type-identity compatibility.
// New consumers use this protocol-independent namespace.
namespace kronuz::io::completion {
using Identity = kronuz::journal::Identity;
using File = kronuz::journal::File;
using DirectoryCursor = kronuz::journal::DirectoryCursor;
using IO = kronuz::journal::IO;
using EntryKind = kronuz::journal::EntryKind;
using EntryFootprint = kronuz::journal::EntryFootprint;
using EntryInspection = kronuz::journal::EntryInspection;
using LeaseMode = kronuz::journal::LeaseMode;
using ManagedCapabilities = kronuz::journal::ManagedCapabilities;
using OwnerLock = kronuz::journal::OwnerLock;
using PosixIO = kronuz::journal::PosixIO;
using kronuz::journal::make_posix_io;
using Token = kronuz::journal::MutationToken;
using Kind = kronuz::journal::PrimitiveKind;
using Request = kronuz::journal::MutationRequest;
using Result = kronuz::journal::MutationCompletion;
using Operation = kronuz::journal::IOOperation;
using Stats = kronuz::journal::CompletionStats;
using kronuz::journal::drive_synchronously;
}
