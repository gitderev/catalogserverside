import js from "@eslint/js";
import globals from "globals";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "public"] },
  {
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      "react-refresh/only-export-components": [
        "warn",
        { allowConstantExport: true },
      ],
      "@typescript-eslint/no-unused-vars": "off",
      "@typescript-eslint/no-explicit-any": "error",
    },
  },
  // Override: allow 'any' as warning in generic data-parsing files
  {
    files: [
      "src/utils/csvMerger.ts",
      "src/utils/txtMerger.ts",
      "src/utils/excelFormatter.ts",
      "src/utils/ean.ts",
      "src/components/DataPreview.tsx",
      "src/components/AltersideCatalogGenerator.tsx",
    ],
    rules: {
      "@typescript-eslint/no-explicit-any": "warn",
    },
  }
);