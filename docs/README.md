# Telegram Scraper Documentation

This directory contains the Jekyll-based documentation site for the Telegram Scraper project.

## Local Development

To run the documentation site locally:

1. **Install Ruby and Bundler** (if not already installed):
   ```bash
   # On macOS with Homebrew
   brew install ruby
   
   # On Ubuntu/Debian
   sudo apt-get install ruby-full build-essential zlib1g-dev
   ```

2. **Install dependencies**:
   ```bash
   cd docs
   bundle install
   ```

3. **Run the Jekyll server**:
   ```bash
   bundle exec jekyll serve
   ```

4. **Open your browser** to `http://localhost:4000`

The site will automatically reload when you make changes to the documentation files.

## Building for Production

To build the static site:

```bash
cd docs
bundle exec jekyll build
```

The generated site will be in the `_site` directory.

## Site Structure

```
docs/
├── _config.yml          # Jekyll configuration
├── _layouts/            # HTML templates
├── _includes/           # Reusable components
├── _sass/              # Sass/SCSS files
├── assets/             # Static assets (CSS, JS, images)
├── index.md            # Homepage
├── getting-started.md  # Getting started guide
├── architecture.md     # Architecture documentation
├── api-reference.md    # API reference
├── examples.md         # Usage examples
├── Gemfile             # Ruby dependencies
└── README.md           # This file
```

## Writing Documentation

### Adding New Pages

1. Create a new `.md` file in the `docs/` directory
2. Add front matter at the top:
   ```yaml
   ---
   layout: default
   title: "Your Page Title"
   ---
   ```
3. Write your content in Markdown
4. Add the page to navigation in `_config.yml` if needed

### Markdown Features

The site supports:
- Standard Markdown syntax
- Code highlighting with syntax highlighting
- Tables
- Custom CSS classes for styling
- Jekyll liquid templating

### Code Blocks

Use fenced code blocks with language specification:

````markdown
```bash
./telegram-scraper --urls "channel1,channel2"
```

```go
type Config struct {
    Platform string `json:"platform"`
    URLs     []string `json:"urls"`
}
```
````

### Custom Styling

The site includes custom CSS in `assets/css/style.scss`. You can:
- Use Bootstrap-like utility classes
- Add custom CSS for specific pages
- Use the predefined color variables

## GitHub Pages Deployment

The site is automatically deployed to GitHub Pages when changes are pushed to the `main` branch. The deployment is handled by the GitHub Action in `.github/workflows/pages.yml`.

### Manual Deployment

If you need to deploy manually:

1. Enable GitHub Pages in your repository settings
2. Set the source to "GitHub Actions"
3. Push changes to trigger the workflow

## Configuration

### Site Configuration

Edit `_config.yml` to customize:
- Site title and description
- Navigation menu
- Social links
- SEO settings
- Jekyll plugins

### Theme Customization

The site uses the Minima theme with extensive customizations:
- Custom colors and typography
- Responsive grid layouts
- Enhanced navigation
- Improved code highlighting

## Contributing

When contributing to the documentation:

1. Follow the existing structure and style
2. Test your changes locally before submitting
3. Use clear, concise language
4. Include code examples where appropriate
5. Update navigation if adding new top-level pages

## Troubleshooting

### Common Issues

**"Bundle install fails"**:
- Ensure you have Ruby 2.7+ installed
- Try `bundle update` if dependencies conflict

**"Jekyll serve fails"**:
- Check for syntax errors in your Markdown
- Verify front matter is properly formatted
- Look for missing dependencies

**"GitHub Pages build fails"**:
- Check the Actions tab for detailed error logs
- Ensure all file paths use lowercase
- Verify Gemfile is compatible with GitHub Pages

### Getting Help

- Check the [Jekyll documentation](https://jekyllrb.com/docs/)
- Review the [GitHub Pages documentation](https://docs.github.com/en/pages)
- Open an issue if you encounter problems

---

Happy documenting! 📚