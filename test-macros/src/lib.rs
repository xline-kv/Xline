use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Stmt};

#[proc_macro_attribute]
pub fn abort_on_panic(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input: syn::ItemFn = parse_macro_input!(item as ItemFn);
    let test_case = &input.sig.ident.to_string();

    let panic_hook: Stmt = syn::parse_quote! {
        std::panic::set_hook(Box::new(|info| {
            let stacktrace = std::backtrace::Backtrace::force_capture();
            println!("`{}` panic! \n@info:\n{}\n@stackTrace:\n{}", #test_case, info, stacktrace);
            std::process::abort();
        }));
    };

    input.block.stmts.insert(0, panic_hook);

    TokenStream::from(quote! {
        #input
    })
}
