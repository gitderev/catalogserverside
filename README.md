# Welcome to your Lovable project

## Project info

**URL**: https://lovable.dev/projects/e73527f5-7da2-4bb3-9603-93fbd746b6c1

## How can I edit this code?

There are several ways of editing your application.

**Use Lovable**

Simply visit the [Lovable Project](https://lovable.dev/projects/e73527f5-7da2-4bb3-9603-93fbd746b6c1) and start prompting.

Changes made via Lovable will be committed automatically to this repo.

**Use your preferred IDE**

If you want to work locally using your own IDE, you can clone this repo and push changes. Pushed changes will also be reflected in Lovable.

The only requirement is having Node.js & npm installed - [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating)

Follow these steps:

```sh
# Step 1: Clone the repository using the project's Git URL.
git clone <YOUR_GIT_URL>

# Step 2: Navigate to the project directory.
cd <YOUR_PROJECT_NAME>

# Step 3: Install the necessary dependencies.
npm i

# Step 4: Start the development server with auto-reloading and an instant preview.
npm run dev
```

**Edit a file directly in GitHub**

- Navigate to the desired file(s).
- Click the "Edit" button (pencil icon) at the top right of the file view.
- Make your changes and commit the changes.

**Use GitHub Codespaces**

- Navigate to the main page of your repository.
- Click on the "Code" button (green button) near the top right.
- Select the "Codespaces" tab.
- Click on "New codespace" to launch a new Codespace environment.
- Edit files directly within the Codespace and commit and push your changes once you're done.

## What technologies are used for this project?

This project is built with:

- Vite
- TypeScript
- React
- shadcn-ui
- Tailwind CSS

## How can I deploy this project?

Simply open [Lovable](https://lovable.dev/projects/e73527f5-7da2-4bb3-9603-93fbd746b6c1) and click on Share -> Publish.

## Can I connect a custom domain to my Lovable project?

Yes, you can!

To connect a domain, navigate to Project > Settings > Domains and click Connect Domain.

Read more here: [Setting up a custom domain](https://docs.lovable.dev/tips-tricks/custom-domain#step-by-step-guide)

## Automated Sync Scheduling (GitHub Actions)

The automated synchronization is triggered every 1 minute by a GitHub Actions workflow (`.github/workflows/cron-tick.yml`). To enable it:

### 1. Configure GitHub Actions Secrets

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret** and add:

| Secret Name | Value |
|---|---|
| `CRON_TICK_URL` | The full URL of the `cron-tick` Edge Function, e.g. `https://<project-id>.supabase.co/functions/v1/cron-tick` |
| `CRON_SECRET` | A strong random string (e.g. generated with `openssl rand -hex 32`) |

### 2. Configure the backend secret

The `CRON_SECRET` value set in GitHub Actions **must match** the `CRON_SECRET` secret configured in your Lovable Cloud (Supabase) project. Set the same value in both places.

### 3. Verify

After committing the workflow, GitHub Actions will call `cron-tick` every 1 minute. The function will check the scheduling configuration (enabled, frequency, daily time) and trigger a sync run if due.
