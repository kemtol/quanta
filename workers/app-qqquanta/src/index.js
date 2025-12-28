// workers/app-quanta/src/index.js
// Serve static assets dari folder ../../futures lewat binding ASSETS

export default {
  async fetch(request, env, ctx) {
    // env.ASSETS disediakan dari [assets] di wrangler.toml
    return env.ASSETS.fetch(request);
  },
};
