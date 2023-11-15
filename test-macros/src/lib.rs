use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Stmt};

#[proc_macro_attribute]
pub fn abort_on_panic(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input: syn::ItemFn = parse_macro_input!(item as ItemFn);

    let panic_hook: Stmt = syn::parse_quote! {
        std::panic::set_hook(Box::new(|info| {
            let stacktrace = std::backtrace::Backtrace::force_capture();
            println!("test panic! \n@info:\n{}\n@stackTrace:\n{}", info, stacktrace);
            std::process::abort();
        }));
    };

    input.block.stmts.insert(0, panic_hook);

    TokenStream::from(quote! {
        #input
    })
}
