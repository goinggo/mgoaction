// Copyright 2014 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The program shows how to use MongoDB and Go to make the collections of data you have today
actionable in a dynamic way.

Ardan Studios
12973 SW 112 ST, Suite 153
Miami, FL 33186
bill@ardanstudios.com

GoingGo.net Post:
http://www.goinggo.net/2013/07/actionable-data-monogdb-go.html
*/
package main

import (
	"github.com/goinggo/mgoaction/engine"
)

func main() {
	// Run the specified rule for the specified userid.
	engine.RunRule("advice", "396bc782-6ac6-4183-a671-6e75ca5989a5")
}
