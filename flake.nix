{
  description = "Logos zcash_wallet_core_module: the Zcash wallet engine (librustzcash, SQLCipher, sync over Tor).";

  inputs = {
    logos-module-builder.url = "github:logos-co/logos-module-builder";
    # OPTIONAL in metadata.json: only its contract is consumed.
    zebrad_module = {
      url = "github:logos-co/logos-zebrad-module";
      inputs.logos-module-builder.follows = "logos-module-builder";
    };
  };

  outputs = inputs@{ self, logos-module-builder, ... }:
    let
      nixpkgs = logos-module-builder.inputs.nixpkgs;
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      # x86_64-windows is a cross build from x86_64-linux.
      targets = systems ++ [ "x86_64-windows" ];
      forAllSystems = f: nixpkgs.lib.genAttrs targets f;
      # Sapling proving parameters ship beside the plugin, fetched at build time with
      # pinned hashes, so a send never downloads them outside the proxy.
      saplingParams = system:
        # The parameter files are data; fetch them with the builder's own package set.
        let pkgs = nixpkgs.legacyPackages.${if system == "x86_64-windows" then "x86_64-linux" else system}; in
        pkgs.runCommand "zcash-sapling-params" {
          spend = pkgs.fetchurl {
            url = "https://download.z.cash/downloads/sapling-spend.params";
            hash = "sha256-jkj/0jq7Ol/ZxViSBPMtnDEoWgS3gJa6QKebdWd+/BM=";
          };
          output = pkgs.fetchurl {
            url = "https://download.z.cash/downloads/sapling-output.params";
            hash = "sha256-Lw67y7m7C8/+laOX5+uonCnrTd5hkcM524hXDj8/sOQ=";
          };
        } ''
          mkdir -p $out/lib
          cp $spend $out/lib/sapling-spend.params
          cp $output $out/lib/sapling-output.params
        '';
    in
    {
      packages = forAllSystems (system:
        (logos-module-builder.lib.mkLogosModule {
          src = ./.;
          configFile = ./metadata.json;
          flakeInputs = inputs;
          externalLibInputs.zcash_sapling_params = saplingParams system;
        }).packages.${system});
    };
}
