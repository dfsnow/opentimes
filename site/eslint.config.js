import js from "@eslint/js";

export default [
  js.configs.all,
  {
    rules: {
      "class-methods-use-this": "off",
      "guard-for-in": "off",
      "id-length": "off",
      "indent": ["error", 2],
      "linebreak-style": ["error", "unix"],
      "max-classes-per-file": "off",
      "max-lines": "off",
      "max-lines-per-function": "off",
      "max-params": "off",
      "max-statements": "off",
      "no-console": "off",
      "no-magic-numbers": "off",
      "no-ternary": "off",
      "no-undef": "off",
      "no-underscore-dangle": "off",
      "no-useless-assignment": "off",
      "one-var": "off",
      "quotes": ["error", "double"],
      "semi": ["error", "always"],
      "sort-vars": "off"
    }
  }
];
