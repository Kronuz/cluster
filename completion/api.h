#pragma once
#include "operation.h"
#include "posix.h"
#include "completion.h"

// Keep the existing canonical types for source and type-identity compatibility.
// New consumers use this protocol-independent namespace.
namespace kronuz::io::completion {
using Identity = kronuz::journal::Identity;
using File = kronuz::journal::File;
using IO = kronuz::journal::IO;
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
