This is a production-grade repository. We can never get away with committing
something that is not fully working with a note that we will fix it later.
Assume this code is contiously deployed and every commit is a realease that has
to work.

In addition, we can never add any workarounds just to make tests pass. For
example, if you add an if statement to detect a test and then return a
hard-coded value, the test gives us false positives and we risk breaking
production. Do not ever – under any cirucmstances – do this. 
