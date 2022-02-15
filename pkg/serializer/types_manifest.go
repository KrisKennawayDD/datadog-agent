// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package serializer

import (
	"github.com/DataDog/agent-payload/v5/manifest"
)

// ManifestMessage is a type alias for manifest payload proto message
type ManifestMessage = manifest.ManifestPayload