# Evidence spanner Source Plugin

Install this plugin in an Evidence app with
```bash
npm install evidence-connector-spanner
```

Register the plugin in your project in your evidence.plugins.yaml file with
```bash
datasources:
  evidence-connector-spanner: {}
```

Launch the development server with `npm run dev` and navigate to the settings menu [localhost:3000/settings](http://localhost:3000/settings) to add a data source using this plugin.

Notes:
- https://www.npmjs.com/package/@google-cloud/spanner
- file-bases-evidence connector
- add ref to advanced-connector
- https://docs.evidence.dev/plugins/create-source-plugin/#promoting-your-plugin
- check TODO's
- write tests (incl. integration tests using emulator)
- create verification pipeline
- create release pipeline
- write README
- publish to npm https://github.com/LeonStoldt/evidence-connector-spanner?tab=readme-ov-file#publishing-to-npm
