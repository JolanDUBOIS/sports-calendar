# Feature Ideas

## Short-term
- [ ] Unified yaml use (either use load_yaml from loader or use yaml package everywhere)
- [ ] Remove dependencies & use sources instead (pipeline config)
- [ ] Create a release branch
    - Will be triggered like prod for a few days before pushing to main
    - Set of tests is triggered each time it is pushed => create tests (unit tests but also more global tests...) -> maybe run cmds and check for ERROR logs...

## Long-term
- [ ] 

## Maybe/Nice-to-have
- [ ] 

## Minor Bugs
- [ ] When reseting, the metadata might not reset properly
- [ ] When running a model (pipeline) and the source doesn't exist, the error should be raised right away instead of falling later when we get empty df of list
