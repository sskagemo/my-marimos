# my marimos

This repo is forked from a template repository from the [marimo](https://marimo.io) team for easy deploy of marimo notebooks and apps using GitHub Pages
I'll update the README as soon as I actually add some of my own notebooks or apps ...

Original README-content from the marimo team is below:

---

This template repository demonstrates how to export [marimo](https://marimo.io) notebooks to WebAssembly and deploy them to GitHub Pages.

## 📚 Included Examples

- `apps/charts.py`: Interactive data visualization with Altair
- `notebooks/fibonacci.py`: Interactive Fibonacci sequence calculator

## 🚀 Usage

1. Fork this repository
2. Add your marimo files to the `notebooks/` or `apps/` directory
   1. `notebooks/` notebooks are exported with `--mode edit`
   2. `apps/` notebooks are exported with `--mode run`
3. Push to main branch
4. Go to repository **Settings > Pages** and change the "Source" dropdown to "GitHub Actions"
5. GitHub Actions will automatically build and deploy to Pages
   (Note: This didn't happen in my case, I had to go into the Actions page and approve the
   workflow first ... and it still hasn't happened actually ...)
