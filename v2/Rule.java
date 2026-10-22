// Copyright 2026 smahajkt
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

public static final class Rule {
	final Scope scope;
	final String key;
	final int limit;
	final long windowMs;

	public Rule(Scope scope, String key, int limit, long windowMs) {
		if (scope == null || key == null || key.isEmpty()) {
			throw new IllegalArgumentException("Scope and key are required");
		}
		if (limit <= 0 || windowMs <= 0) {
			throw new IllegalArgumentException(
					"Limit and window must be positive");
		}

		this.scope = scope;
		this.key = key;
		this.limit = limit;
		this.windowMs = windowMs;
	}
}